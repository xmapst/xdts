package iproto

import "fmt"

const maxSnapshotChunkNumber uint64 = 1<<63 - 2

func SnapshotProgressCounts(progress *SnapshotProgress) (uint64, uint64, error) {
	if progress == nil || progress.GetTotalChunks() == 0 || progress.GetTotalChunks() > maxSnapshotChunkNumber || progress.GetDoneChunks() > progress.GetTotalChunks() {
		return 0, 0, fmt.Errorf("iproto: invalid base snapshot counts")
	}
	ext := progress.GetExtension()
	if ext == nil {
		return progress.GetTotalChunks(), progress.GetDoneChunks(), nil
	}
	if ext.GetSequence() == 0 || ext.GetTotalChunks() <= progress.GetTotalChunks() || ext.GetTotalChunks() > maxSnapshotChunkNumber || ext.GetSequence() > ext.GetTotalChunks()-progress.GetTotalChunks() || ext.GetDoneChunks() > ext.GetTotalChunks() || progress.GetDoneChunks() != min(ext.GetDoneChunks(), progress.GetTotalChunks()) {
		return 0, 0, fmt.Errorf("iproto: invalid snapshot extension counts")
	}
	return ext.GetTotalChunks(), ext.GetDoneChunks(), nil
}

func ValidateSnapshotEndPlan(end *SnapshotEnd) error {
	if end == nil || end.GetTotalChunks() == 0 || end.GetTotalChunks() > maxSnapshotChunkNumber {
		return fmt.Errorf("iproto: invalid snapshot final total")
	}
	if end.Recovery != nil {
		if err := ValidateSnapshotRecoveryIdentity(end.Recovery); err != nil {
			return err
		}
		if end.TotalChunks >= 1<<63-4002 {
			return fmt.Errorf("iproto: invalid recovery final chunk range")
		}
	}
	if err := ValidateSnapshotRecoveryTables(end.RecoveryTables, true); err != nil {
		return err
	}
	base, seq := end.GetInitialTotalChunks(), end.GetPlanExtensionSequence()
	if base == 0 && seq == 0 {
		return nil
	}
	if base == 0 || seq == 0 || base >= end.GetTotalChunks() || seq > end.GetTotalChunks()-base {
		return fmt.Errorf("iproto: invalid snapshot final extension")
	}
	return nil
}
