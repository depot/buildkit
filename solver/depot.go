package solver

import (
	"fmt"
	"time"

	"github.com/moby/buildkit/util/progress"
)

// DEPOT: Report merged edges to progress writer. This is important as it is confusing when merges happen.
// This reaches into private fields of the edge and sharedOp structs to get the progress writer.
func depotReportMergedEdges(e *edge) {
	op, ok := e.op.(*sharedOp)
	if ok {
		if op.st != nil && op.st.mpw != nil {
			writer := op.st.mpw
			now := time.Now()
			status := progress.Status{Action: "Merging", Started: &now, Completed: &now}
			msg := fmt.Sprintf("Deduplicating step ID %s, another build is calculating it", e.edge.Vertex.Name())
			_ = writer.Write(msg, status)
		}
	}
}
