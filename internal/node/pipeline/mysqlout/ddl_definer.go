package mysqlout

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/schemamig"
)

// incrementalSchemaOptions checks the account named by this CREATE or ALTER EVENT, rather
// than reusing an availability decision from a different object or version.
func (w *Writer) incrementalSchemaOptions(ctx context.Context, schema, statement string) (schemamig.Options, error) {
	options := w.opts.SchemaOptions
	if fallback := w.opts.DDLPolicy.GetDefinerFallback(); fallback != "" {
		options.DefinerFallback = fallback
	}
	kind, _, _, err := schemamig.CreationIdentity(schema, statement)
	var account *schemamig.DefinerAccount
	if err != nil {
		account, err = schemamig.AlterEventDefiner(statement)
		if err == nil && account == nil {
			account, err = schemamig.AlterViewDefiner(statement)
		}
	} else {
		account, err = schemamig.DefinitionDefiner(kind, statement)
	}
	if err != nil {
		return options, err
	}
	if account == nil || account.CurrentUser {
		options.DefinerAccountExists = true
		return options, nil
	}
	lookup, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	exists, err := w.ddlDefinerExists(lookup, *account)
	if err != nil {
		return options, err
	}
	options.DefinerAccountExists = exists
	if !exists && options.DefinerFallback != "" {
		fallback, err := schemamig.ParseDefinerAccount(options.DefinerFallback)
		if err != nil {
			return options, err
		}
		if valid, err := w.ddlDefinerExists(lookup, fallback); err != nil {
			return options, err
		} else if !valid {
			return options, fmt.Errorf("mysqlout: configured DDL definer fallback account is absent")
		}
	}
	return options, nil
}

func (w *Writer) ddlDefinerExists(ctx context.Context, account schemamig.DefinerAccount) (bool, error) {
	if account.CurrentUser {
		return true, nil
	}
	var own bool
	if err := w.conn.QueryRowContext(ctx, "SELECT BINARY CURRENT_USER() = BINARY CONCAT(?, '@', ?)", account.User, account.Host).Scan(&own); err != nil {
		return false, err
	}
	if own {
		return true, nil
	}
	var exists bool
	if err := w.conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM mysql.user WHERE BINARY User=BINARY ? AND BINARY Host=BINARY ?)", account.User, account.Host).Scan(&exists); err != nil {
		return false, fmt.Errorf("mysqlout: cannot verify incremental DDL definer: %w", err)
	}
	return exists, nil
}
