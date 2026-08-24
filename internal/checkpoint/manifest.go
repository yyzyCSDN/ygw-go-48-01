package checkpoint

// Manifest describes one durable checkpoint: the state snapshot sequence and
// the source offset it covers. The coordinator writes a manifest only after
// both the state snapshot and the sink flush have succeeded.
type Manifest struct {
	Seq    int64
	Offset int64
}

// NewManifest builds a manifest from a snapshot sequence and an offset.
func NewManifest(seq int64, offset int64) Manifest {
	return Manifest{Seq: seq, Offset: offset}
}

// Valid reports whether the manifest covers a non-negative offset.
func (m Manifest) Valid() bool {
	return m.Offset >= 0 && m.Seq >= 0
}
