//go:build !linux

package strata

import "runtime"

type osMemSample struct {
	RSSBytes     uint64
	PSSBytes     uint64
	RssAnonBytes uint64
	RssFileBytes uint64
	VmHWMBytes   uint64
	Available    bool
}

func sampleOSMem() osMemSample { return osMemSample{} }

func sampleCleanMem() (runtime.MemStats, osMemSample) {
	runtime.GC()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem, osMemSample{}
}
