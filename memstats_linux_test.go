//go:build linux

package strata

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

type osMemSample struct {
	RSSBytes     uint64
	PSSBytes     uint64
	RssAnonBytes uint64
	RssFileBytes uint64
	VmHWMBytes   uint64
	Available    bool
}

func parseProcKB(raw, prefix string) (uint64, bool) {
	for line := range strings.SplitSeq(raw, "\n") {
		after, ok := strings.CutPrefix(line, prefix)
		if !ok {
			continue
		}
		fields := strings.Fields(after)
		if len(fields) == 0 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

func sampleOSMem() osMemSample {
	var out osMemSample
	if raw, err := os.ReadFile("/proc/self/smaps_rollup"); err == nil {
		text := string(raw)
		if v, ok := parseProcKB(text, "Rss:"); ok {
			out.RSSBytes = v
			out.Available = true
		}
		if v, ok := parseProcKB(text, "Pss:"); ok {
			out.PSSBytes = v
			out.Available = true
		}
		if v, ok := parseProcKB(text, "Pss_Anon:"); ok {
			out.RssAnonBytes = v
		}
		if v, ok := parseProcKB(text, "Pss_File:"); ok {
			out.RssFileBytes = v
		}
	}
	if raw, err := os.ReadFile("/proc/self/status"); err == nil {
		text := string(raw)
		if v, ok := parseProcKB(text, "VmRSS:"); ok && out.RSSBytes == 0 {
			out.RSSBytes = v
			out.Available = true
		}
		if v, ok := parseProcKB(text, "VmHWM:"); ok {
			out.VmHWMBytes = v
			out.Available = true
		}
		if v, ok := parseProcKB(text, "RssAnon:"); ok {
			out.RssAnonBytes = v
		}
		if v, ok := parseProcKB(text, "RssFile:"); ok {
			out.RssFileBytes = v
		}
	}
	return out
}

func sampleCleanMem() (runtime.MemStats, osMemSample) {
	runtime.GC()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return mem, sampleOSMem()
}
