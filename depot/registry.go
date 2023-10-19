package depot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	digest "github.com/opencontainers/go-digest"
)

var (
	ErrAlreadyExists = errors.New("already exists")
)

type UploadSession struct {
	kind   UploadKind
	digest digest.Digest

	session string
	parts   struct {
		mu  sync.Mutex
		ids []string
	}
}

func NewUploadSession(kind UploadKind, dgst digest.Digest) *UploadSession {
	return &UploadSession{
		kind:   kind,
		digest: dgst,
	}
}

func (s *UploadSession) SessionID() string {
	return s.session
}

func (s *UploadSession) Create(ctx context.Context) error {
	createSession := struct {
		Kind        string `json:"kind"`
		Digest      string `json:"digest"`
		ContentType string `json:"contentType"`
	}{
		Kind:   s.kind.String(),
		Digest: s.digest.String(),
	}

	if s.kind == UploadKindManifest {
		createSession.ContentType = "application/vnd.oci.image.manifest.v1+json"
	} else if s.kind == UploadKindConfig {
		createSession.ContentType = "application/vnd.oci.image.config.v1+json"
	} else if s.kind == UploadKindBlob {
		createSession.ContentType = "application/vnd.oci.image.layer.v1.tar + gzip"
	}

	buf, err := json.Marshal(&createSession)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://load.depot.dev/_/upload", bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	bearer := BearerFromEnv()
	req.Header.Set("Authorization", bearer)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var session struct {
			Session string `json:"session"`
		}

		err := json.NewDecoder(resp.Body).Decode(&session)
		if err != nil {
			return err
		}

		s.session = session.Session
		return nil
	case http.StatusNotModified:
		return ErrAlreadyExists
	}

	var errorBody ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&errorBody)
	if err != nil {
		return err
	}

	return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, errorBody.Error())
}

func (s *UploadSession) UploadPart(ctx context.Context, part int, r io.Reader) error {
	url := fmt.Sprintf("https://load.depot.dev/_/upload/%s/part/%d", s.session, part)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return err
	}

	bearer := BearerFromEnv()
	req.Header.Set("Authorization", bearer)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var part struct {
			Part string `json:"part"`
		}

		err := json.NewDecoder(resp.Body).Decode(&part)
		if err != nil {
			return err
		}

		s.parts.mu.Lock()
		s.parts.ids = append(s.parts.ids, part.Part)
		s.parts.mu.Unlock()

		return nil
	case http.StatusNotModified:
		return ErrAlreadyExists
	}

	var errorBody ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&errorBody)
	if err != nil {
		return err
	}

	return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, errorBody.Error())
}

func (s *UploadSession) Complete(ctx context.Context) error {
	var completeSession struct {
		Parts []string `json:"parts"`
	}
	{
		s.parts.mu.Lock()
		completeSession.Parts = s.parts.ids
		s.parts.mu.Unlock()
	}

	buf, err := json.Marshal(completeSession)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://load.depot.dev/_/upload/%s", s.session)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	bearer := BearerFromEnv()
	req.Header.Set("Authorization", bearer)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		return nil
	}

	var errorBody ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&errorBody)
	if err != nil {
		return err
	}

	return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, errorBody.Error())
}

func (s *UploadSession) Abort(ctx context.Context) error {
	url := fmt.Sprintf("https://load.depot.dev/_/upload/%s", s.session)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	bearer := BearerFromEnv()
	req.Header.Set("Authorization", bearer)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted {
		return nil
	}

	var errorBody ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&errorBody)
	if err != nil {
		return err
	}

	return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, errorBody.Error())
}

type UploadKind int

const (
	UploadKindManifest UploadKind = iota
	UploadKindConfig
	UploadKindBlob
)

func (k UploadKind) String() string {
	switch k {
	case UploadKindManifest:
		return "manifest"
	case UploadKindConfig, UploadKindBlob:
		return "blob"
	default:
		return "unknown"
	}
}

type ErrorResponse struct {
	Errors []Error `json:"errors"`
}

func (e ErrorResponse) Error() string {
	if len(e.Errors) == 0 {
		return ""
	}

	var errs []string
	for _, err := range e.Errors {
		errs = append(errs, err.Error())
	}

	return strings.Join(errs, ", ")
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
