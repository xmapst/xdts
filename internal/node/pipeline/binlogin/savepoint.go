package binlogin

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

const maxTransactionSavepoints = 1024

type transactionSavepoint struct {
	name string
	rows int
}

// feedSavepoint accepts only one parsed savepoint statement inside BEGIN.
// Savepoint names are case insensitive in MySQL; later savepoints disappear
// when rolling back to an earlier one, including marks at the same row offset.
func (a *aggregator) feedSavepoint(query string) (bool, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "SAVEPOINT") && !strings.HasPrefix(upper, "RELEASE") && !strings.HasPrefix(upper, "ROLLBACK") {
		return false, nil
	}
	stmt, err := parser.New().ParseOneStmt(query, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		return false, fmt.Errorf("binlogin: invalid transaction savepoint statement")
	}
	name := ""
	action := 0
	switch s := stmt.(type) {
	case *ast.SavepointStmt:
		name = s.Name
		action = 1
	case *ast.ReleaseSavepointStmt:
		name = s.Name
		action = 2
	case *ast.RollbackStmt:
		if s.SavepointName != "" {
			name = s.SavepointName
			action = 3
		}
	}
	if action == 0 {
		return false, nil
	}
	name = strings.ToLower(name)
	index := -1
	for i, mark := range a.savepoints {
		if mark.name == name {
			index = i
			break
		}
	}
	switch action {
	case 1:
		if index >= 0 {
			a.savepoints = append(a.savepoints[:index], a.savepoints[index+1:]...)
		}
		if len(a.savepoints) >= maxTransactionSavepoints {
			return true, fmt.Errorf("binlogin: transaction savepoint limit exceeded")
		}
		a.savepoints = append(a.savepoints, transactionSavepoint{name: name, rows: len(a.rows)})
	case 2:
		if index < 0 {
			return true, fmt.Errorf("binlogin: release referenced an unknown savepoint")
		}
		a.savepoints = append(a.savepoints[:index], a.savepoints[index+1:]...)
	case 3:
		if index < 0 {
			return true, fmt.Errorf("binlogin: rollback referenced an unknown savepoint")
		}
		offset := a.savepoints[index].rows
		clear(a.rows[offset:])
		a.rows = a.rows[:offset]
		a.savepoints = a.savepoints[:index+1]
	}
	return true, nil
}
