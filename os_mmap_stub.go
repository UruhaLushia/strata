//go:build windows || ios

package strata

func mapPrefix(_ *osFile, _ uint64, _ bool) (mapView, error) { return nil, nil }

type stubMap struct{}

func (*stubMap) bytes() []byte   { return nil }
func (*stubMap) adviseDontNeed() {}
func (*stubMap) close() error    { return nil }
