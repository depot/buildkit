package depot

// LeaseLabel identifies the lease's ID during an export of the image.
// I happen to use the session ID as it is unique to this specific export.
const ExportLeaseLabel = "depot/session.id"
