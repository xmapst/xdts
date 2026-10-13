package iproto

import (
	"errors"
	"strings"
	"unicode/utf8"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
)

func ValidateSnapshotRecoveryIdentity(identity *SnapshotRecoveryIdentity) error {
	if identity == nil || identity.Sequence == 0 || identity.Sequence > 4096 {
		return errors.New("iproto: invalid table recovery sequence")
	}
	id, err := uuid.Parse(identity.Uuid)
	if err != nil || id == uuid.Nil() || id.String() != identity.Uuid {
		return errors.New("iproto: invalid table recovery UUID")
	}
	return nil
}

func ValidateSnapshotRecoveryTables(tables []*SnapshotRecoveryTable, allowEmpty bool) error {
	if len(tables) > 4000 || len(tables) == 0 && !allowEmpty {
		return errors.New("iproto: invalid table recovery scope")
	}
	seen := map[[2]string]bool{}
	for _, table := range tables {
		key := [2]string{table.GetSchema(), table.GetTable()}
		for _, name := range key {
			if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 64 || strings.ContainsRune(name, 0) {
				return errors.New("iproto: invalid table recovery name")
			}
		}
		if seen[key] {
			return errors.New("iproto: duplicate table recovery name")
		}
		seen[key] = true
	}
	return nil
}

func ValidateSnapshotRecoveryBegin(begin *SnapshotRecoveryBegin) error {
	if begin.GetPipelineId() == "" {
		return errors.New("iproto: missing recovery pipeline")
	}
	if err := ValidateSnapshotRecoveryIdentity(begin.GetRecovery()); err != nil {
		return err
	}
	if err := ValidateSnapshotRecoveryTables(begin.GetTables(), false); err != nil {
		return err
	}
	_, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, begin.GetLowGtidSet())
	return err
}

// Both messages are passed by independent ownership between target workflows.
// Permit/Result carry completion only, never shared mutable recovery state.
type SnapshotRecoveryRequest struct {
	Begin  *SnapshotRecoveryBegin
	Permit chan error
}
type SnapshotRecoveryCompletion struct {
	End    *SnapshotEnd
	Result chan error
}
