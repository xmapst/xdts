package probe

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	driver "github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func validDefinerAccounts(accounts []*iproto.DefinerAccount) error {
	if len(accounts) > 100 {
		return errors.New("account scope exceeds limit")
	}
	for _, a := range accounts {
		if a == nil || a.GetUser() == "" || a.GetHost() == "" || !utf8.ValidString(a.GetUser()) || !utf8.ValidString(a.GetHost()) || utf8.RuneCountInString(a.GetUser()) > 32 || len(a.GetHost()) > 255 || strings.ContainsRune(a.GetUser(), 0) || strings.ContainsRune(a.GetHost(), 0) {
			return errors.New("invalid account identity")
		}
	}
	return nil
}

func readDefinerAccounts(ctx context.Context, conn *sql.Conn, current string, accounts []*iproto.DefinerAccount, schemas ...[]string) ([]*iproto.DefinerAccountResult, error) {
	results := make([]*iproto.DefinerAccountResult, 0, len(accounts))
	for _, a := range accounts {
		r := &iproto.DefinerAccountResult{Account: &iproto.DefinerAccount{User: a.GetUser(), Host: a.GetHost()}}
		if a.GetUser()+"@"+a.GetHost() == current {
			r.Verified = true
			r.Exists = true
			if err := readAccountExecute(ctx, conn, r, schemas); err != nil {
				return nil, err
			}
			results = append(results, r)
			continue
		}
		var present int
		err := conn.QueryRowContext(ctx, "SELECT 1 FROM mysql.user WHERE BINARY User = ? AND BINARY Host = ? LIMIT 1", a.GetUser(), a.GetHost()).Scan(&present)
		if err == nil {
			r.Verified = true
			r.Exists = true
		} else if errors.Is(err, sql.ErrNoRows) {
			r.Verified = true
		} else {
			var mysqlErr *driver.MySQLError
			if !errors.As(err, &mysqlErr) || (mysqlErr.Number != 1142 && mysqlErr.Number != 1143 && mysqlErr.Number != 1044 && mysqlErr.Number != 1227) {
				return nil, err
			}
			// Permission denial cannot prove absence. Keep an explicit unknown result.
		}
		if r.Verified && r.Exists {
			if err := readAccountExecute(ctx, conn, r, schemas); err != nil {
				return nil, err
			}
		}
		results = append(results, r)
	}
	return results, nil
}

func readAccountExecute(ctx context.Context, conn *sql.Conn, result *iproto.DefinerAccountResult, scopes [][]string) error {
	rows, err := conn.QueryContext(ctx, "SHOW GRANTS FOR "+quoteDefinitionName(result.GetAccount().GetUser())+"@"+quoteDefinitionName(result.GetAccount().GetHost()))
	if err != nil {
		var mysqlErr *driver.MySQLError
		if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1142 || mysqlErr.Number == 1143 || mysqlErr.Number == 1044 || mysqlErr.Number == 1227) {
			return nil
		}
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	grants := []string{}
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	result.GrantsVerified = true
	for _, schemas := range scopes {
		for _, schema := range schemas {
			if schemamig.GrantsSchemaExecute(grants, schema) {
				result.ExecuteSchemas = append(result.ExecuteSchemas, schema)
			}
		}
	}
	return nil
}
