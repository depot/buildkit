package depot

// DEPOT: Imported from containerd with some small refactors.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"
	"time"

	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/klauspost/pgzip"
	"github.com/minio/sha256-simd"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"github.com/vbatts/tar-split/archive/tar"
)

var _ estargz.Compressor = (*GzipCompressor)(nil)

func NewGzipCompressor() *GzipCompressor {
	return &GzipCompressor{pgzip.BestCompression}
}

func NewGzipCompressorWithLevel(level int) *GzipCompressor {
	return &GzipCompressor{level}
}

type GzipCompressor struct {
	compressionLevel int
}

func (gc *GzipCompressor) Writer(w io.Writer) (estargz.WriteFlushCloser, error) {
	return pgzip.NewWriterLevel(w, gc.compressionLevel)
}

func (gc *GzipCompressor) WriteTOCAndFooter(w io.Writer, off int64, toc *estargz.JTOC, diffHash hash.Hash) (digest.Digest, error) {
	tocJSON, err := json.MarshalIndent(toc, "", "\t")
	if err != nil {
		return "", err
	}
	gz, _ := pgzip.NewWriterLevel(w, gc.compressionLevel)
	gw := io.Writer(gz)
	if diffHash != nil {
		gw = io.MultiWriter(gz, diffHash)
	}
	tw := tar.NewWriter(gw)
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     estargz.TOCTarName,
		Size:     int64(len(tocJSON)),
	}); err != nil {
		return "", err
	}
	if _, err := tw.Write(tocJSON); err != nil {
		return "", err
	}

	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	if _, err := w.Write(gzipFooterBytes(off)); err != nil {
		return "", err
	}
	return digest.FromBytes(tocJSON), nil
}

// gzipFooterBytes returns the 51 bytes footer.
func gzipFooterBytes(tocOff int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, estargz.FooterSize))
	gz, _ := pgzip.NewWriterLevel(buf, pgzip.NoCompression) // MUST be NoCompression to keep 51 bytes

	// Extra header indicating the offset of TOCJSON
	// https://tools.ietf.org/html/rfc1952#section-2.3.1.1
	header := make([]byte, 0, 4)
	header = append(header, 'S', 'G')
	subfield := fmt.Sprintf("%016xSTARGZ", tocOff)
	binary.LittleEndian.AppendUint16(header[2:4], uint16(len(subfield))) // little-endian per RFC1952
	gz.Header.Extra = append(header, []byte(subfield)...)
	gz.Close()
	if buf.Len() != estargz.FooterSize {
		panic(fmt.Sprintf("footer buffer = %d, not %d", buf.Len(), estargz.FooterSize))
	}
	return buf.Bytes()
}

// A Writer writes stargz files.
//
// Use NewWriter to create a new Writer.
type Writer struct {
	bw       *bufio.Writer
	cw       *countWriter
	toc      *estargz.JTOC
	diffHash hash.Hash // SHA-256 of uncompressed tar

	closed        bool
	gz            io.WriteCloser
	lastUsername  map[int]string
	lastGroupname map[int]string
	compressor    estargz.Compressor

	uncompressedCounter *countWriteFlusher

	// ChunkSize optionally controls the maximum number of bytes
	// of data of a regular file that can be written in one gzip
	// stream before a new gzip stream is started.
	// Zero means to use a default, currently 4 MiB.
	ChunkSize int

	// MinChunkSize optionally controls the minimum number of bytes
	// of data must be written in one gzip stream before a new gzip
	// NOTE: This adds a TOC property that stargz snapshotter < v0.13.0 doesn't understand.
	MinChunkSize int

	needsOpenGzEntries map[string]struct{}
}

// NewWriterWithCompressor returns a new stargz writer writing to w.
// The compression method is configurable.
//
// The writer must be closed to write its trailing table of contents.
func NewWriterWithCompressor(w io.Writer, c estargz.Compressor) *Writer {
	bw := bufio.NewWriter(w)
	cw := &countWriter{w: bw}
	return &Writer{
		bw:                  bw,
		cw:                  cw,
		toc:                 &estargz.JTOC{Version: 1},
		diffHash:            sha256.New(),
		compressor:          c,
		uncompressedCounter: &countWriteFlusher{},
	}
}

// Close writes the stargz's table of contents and flushes all the
// buffers, returning any error.
func (w *Writer) Close() (digest.Digest, error) {
	if w.closed {
		return "", nil
	}
	defer func() { w.closed = true }()

	if w.closed {
		return "", errors.New("write on closed Writer")
	}
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return "", err
		}
		w.gz = nil
	}

	// Write the TOC index and footer.
	tocDigest, err := w.compressor.WriteTOCAndFooter(w.cw, w.cw.n, w.toc, w.diffHash)
	if err != nil {
		return "", err
	}
	if err := w.bw.Flush(); err != nil {
		return "", err
	}

	return tocDigest, nil
}

// AppendTarLossLess reads the tar or tar.gz file from r and appends
// each of its contents to w.
//
// The input r can optionally be gzip compressed but the output will
// always be compressed by the specified compressor.
//
// The difference of this func with AppendTar is that this writes
// the input tar stream into w without any modification (e.g. to header bytes).
//
// Note that if the input tar stream already contains TOC JSON, this returns
// error because w cannot overwrite the TOC JSON to the one generated by w without
// lossy modification. To avoid this error, if the input stream is known to be stargz/estargz,
// you shoud decompress it and remove TOC JSON in advance.
func (w *Writer) AppendTarLossLess(r io.Reader) error {
	lossless := true

	var src io.Reader
	br := bufio.NewReader(r)
	if isGzip(br) {
		zr, _ := pgzip.NewReader(br)
		src = zr
	} else {
		src = io.Reader(br)
	}
	dst := currentCompressionWriter{w}
	var tw *tar.Writer
	if !lossless {
		tw = tar.NewWriter(dst) // use tar writer only when this isn't lossless mode.
	}
	tr := tar.NewReader(src)
	if lossless {
		tr.RawAccounting = true
	}
	prevOffset := w.cw.n
	var prevOffsetUncompressed int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			if lossless {
				if remain := tr.RawBytes(); len(remain) > 0 {
					// Collect the remaining null bytes.
					// https://github.com/vbatts/tar-split/blob/80a436fd6164c557b131f7c59ed69bd81af69761/concept/main.go#L49-L53
					if _, err := dst.Write(remain); err != nil {
						return err
					}
				}
			}
			break
		}
		if err != nil {
			return errors.Errorf("error reading from source tar: tar.Reader.Next: %v", err)
		}

		cleanEntryName := strings.TrimPrefix(path.Clean("/"+h.Name), "/")
		if cleanEntryName == estargz.TOCTarName {
			// It is possible for a layer to be "stargzified" twice during the
			// distribution lifecycle. So we reserve "TOCTarName" here to avoid
			// duplicated entries in the resulting layer.
			if lossless {
				// We cannot handle this in lossless way.
				return errors.Errorf("existing TOC JSON is not allowed; decompress layer before append")
			}
			continue
		}

		xattrs := make(map[string][]byte)
		const xattrPAXRecordsPrefix = "SCHILY.xattr."
		if h.PAXRecords != nil {
			for k, v := range h.PAXRecords {
				if strings.HasPrefix(k, xattrPAXRecordsPrefix) {
					xattrs[k[len(xattrPAXRecordsPrefix):]] = []byte(v)
				}
			}
		}
		ent := &estargz.TOCEntry{
			Name:        h.Name,
			Mode:        h.Mode,
			UID:         h.Uid,
			GID:         h.Gid,
			Uname:       w.nameIfChanged(&w.lastUsername, h.Uid, h.Uname),
			Gname:       w.nameIfChanged(&w.lastGroupname, h.Gid, h.Gname),
			ModTime3339: formatModtime(h.ModTime),
			Xattrs:      xattrs,
		}
		if err := w.condOpenGz(); err != nil {
			return err
		}
		if tw != nil {
			if err := tw.WriteHeader(h); err != nil {
				return err
			}
		} else {
			if _, err := dst.Write(tr.RawBytes()); err != nil {
				return err
			}
		}
		switch h.Typeflag {
		case tar.TypeLink:
			ent.Type = "hardlink"
			ent.LinkName = h.Linkname
		case tar.TypeSymlink:
			ent.Type = "symlink"
			ent.LinkName = h.Linkname
		case tar.TypeDir:
			ent.Type = "dir"
		case tar.TypeReg:
			ent.Type = "reg"
			ent.Size = h.Size
		case tar.TypeChar:
			ent.Type = "char"
			ent.DevMajor = int(h.Devmajor)
			ent.DevMinor = int(h.Devminor)
		case tar.TypeBlock:
			ent.Type = "block"
			ent.DevMajor = int(h.Devmajor)
			ent.DevMinor = int(h.Devminor)
		case tar.TypeFifo:
			ent.Type = "fifo"
		default:
			return errors.Errorf("unsupported input tar entry %q", h.Typeflag)
		}

		// We need to keep a reference to the TOC entry for regular files, so that we
		// can fill the digest later.
		var regFileEntry *estargz.TOCEntry
		var payloadDigest digest.Digester
		if h.Typeflag == tar.TypeReg {
			regFileEntry = ent
			// DEPOT: Using SIMD digester here.
			payloadDigest = NewFastDigester()
		}

		if h.Typeflag == tar.TypeReg && ent.Size > 0 {
			var written int64
			totalSize := ent.Size // save it before we destroy ent
			tee := io.TeeReader(tr, payloadDigest.Hash())
			for written < totalSize {
				chunkSize := int64(w.chunkSize())
				remain := totalSize - written
				if remain < chunkSize {
					chunkSize = remain
				} else {
					ent.ChunkSize = chunkSize
				}

				// We flush the underlying compression writer here to correctly calculate "w.cw.n".
				if err := w.flushGz(); err != nil {
					return err
				}
				if w.needsOpenGz(ent) || w.cw.n-prevOffset >= int64(w.MinChunkSize) {
					if err := w.closeGz(); err != nil {
						return err
					}
					ent.Offset = w.cw.n
					prevOffset = ent.Offset
					prevOffsetUncompressed = w.uncompressedCounter.n
				} else {
					ent.Offset = prevOffset
					ent.InnerOffset = w.uncompressedCounter.n - prevOffsetUncompressed
				}

				ent.ChunkOffset = written
				// DEPOT: Using SIMD digester here.
				chunkDigest := NewFastDigester()

				if err := w.condOpenGz(); err != nil {
					return err
				}

				teeChunk := io.TeeReader(tee, chunkDigest.Hash())
				var out io.Writer
				if tw != nil {
					out = tw
				} else {
					out = dst
				}
				if _, err := io.CopyN(out, teeChunk, chunkSize); err != nil {
					return errors.Errorf("error copying %q: %v", h.Name, err)
				}
				ent.ChunkDigest = chunkDigest.Digest().String()
				w.toc.Entries = append(w.toc.Entries, ent)
				written += chunkSize
				ent = &estargz.TOCEntry{
					Name: h.Name,
					Type: "chunk",
				}
			}
		} else {
			w.toc.Entries = append(w.toc.Entries, ent)
		}
		if payloadDigest != nil {
			regFileEntry.Digest = payloadDigest.Digest().String()
		}
		if tw != nil {
			if err := tw.Flush(); err != nil {
				return err
			}
		}
	}
	remainDest := io.Discard
	if lossless {
		remainDest = dst // Preserve the remaining bytes in lossless mode
	}
	_, err := io.Copy(remainDest, src)
	return err
}

func (w *Writer) condOpenGz() (err error) {
	if w.gz == nil {
		w.gz, err = w.compressor.Writer(w.cw)
		if w.gz != nil {
			w.gz = w.uncompressedCounter.register(w.gz)
		}
	}
	return
}

func (w *Writer) needsOpenGz(ent *estargz.TOCEntry) bool {
	if ent.Type != "reg" {
		return false
	}
	if w.needsOpenGzEntries == nil {
		return false
	}
	_, ok := w.needsOpenGzEntries[ent.Name]
	return ok
}

func (w *Writer) closeGz() error {
	if w.closed {
		return errors.New("write on closed Writer")
	}
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return err
		}
		w.gz = nil
	}
	return nil
}

func (w *Writer) flushGz() error {
	if w.closed {
		return errors.New("flush on closed Writer")
	}
	if w.gz != nil {
		if f, ok := w.gz.(interface {
			Flush() error
		}); ok {
			return f.Flush()
		}
	}
	return nil
}

// nameIfChanged returns name, unless it was the already the value of (*mp)[id],
// in which case it returns the empty string.
func (w *Writer) nameIfChanged(mp *map[int]string, id int, name string) string {
	if name == "" {
		return ""
	}
	if *mp == nil {
		*mp = make(map[int]string)
	}
	if (*mp)[id] == name {
		return ""
	}
	(*mp)[id] = name
	return name
}

func (w *Writer) chunkSize() int {
	if w.ChunkSize <= 0 {
		return 4 << 20
	}
	return w.ChunkSize
}

// isGzip reports whether br is positioned right before an upcoming gzip stream.
// It does not consume any bytes from br.
func isGzip(br *bufio.Reader) bool {
	const (
		gzipID1     = 0x1f
		gzipID2     = 0x8b
		gzipDeflate = 8
	)
	peek, _ := br.Peek(3)
	return len(peek) >= 3 && peek[0] == gzipID1 && peek[1] == gzipID2 && peek[2] == gzipDeflate
}

// countWriter counts how many bytes have been written to its wrapped
// io.Writer.
type countWriter struct {
	w io.Writer
	n int64
}

func (cw *countWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return
}

type countWriteFlusher struct {
	io.WriteCloser
	n int64
}

func (wc *countWriteFlusher) register(w io.WriteCloser) io.WriteCloser {
	wc.WriteCloser = w
	return wc
}

// currentCompressionWriter writes to the current w.gz field, which can
// change throughout writing a tar entry.
//
// Additionally, it updates w's SHA-256 of the uncompressed bytes
// of the tar file.
type currentCompressionWriter struct{ w *Writer }

func (ccw currentCompressionWriter) Write(p []byte) (int, error) {
	ccw.w.diffHash.Write(p)
	if ccw.w.gz == nil {
		if err := ccw.w.condOpenGz(); err != nil {
			return 0, err
		}
	}
	return ccw.w.gz.Write(p)
}

func formatModtime(t time.Time) string {
	if t.IsZero() || t.Unix() == 0 {
		return ""
	}
	return t.UTC().Round(time.Second).Format(time.RFC3339)
}
