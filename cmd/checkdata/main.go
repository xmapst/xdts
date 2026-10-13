package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/xmapst/xdts/internal/datacheck"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	cancel()
	os.Exit(code)
}

func openDatabase(dsn string) (*sql.DB, error) {
	config, err := driver.ParseDSN(dsn)
	if err != nil || dsn == "" {
		return nil, datacheck.ErrConfig
	}
	// Fixed session representation and network bounds make byte comparisons
	// independent of caller DSN date parsing and session timezone options.
	config.ParseTime = false
	config.MultiStatements = false
	config.AllowAllFiles = false
	config.Timeout, config.ReadTimeout, config.WriteTimeout = 5*time.Second, 30*time.Second, 5*time.Second
	config.MaxAllowedPacket = 16 * 1024 * 1024
	if config.Params == nil {
		config.Params = map[string]string{}
	}
	config.Params["time_zone"] = "'+00:00'"
	connector, err := driver.NewConnector(config)
	if err != nil {
		return nil, datacheck.ErrConfig
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func run(parent context.Context, args []string, getenv func(string) string, output, errorOutput io.Writer) int {
	flags := flag.NewFlagSet("checkdata", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var file, sourceEnv, targetEnv string
	var timeout time.Duration
	var modulo uint64
	var details int
	flags.StringVar(&file, "config", "", "JSON table mapping file")
	flags.StringVar(&sourceEnv, "source-dsn-env", "XDTS_CHECK_SOURCE_DSN", "source DSN environment key")
	flags.StringVar(&targetEnv, "target-dsn-env", "XDTS_CHECK_TARGET_DSN", "target DSN environment key")
	flags.DurationVar(&timeout, "timeout", 10*time.Minute, "overall deadline")
	flags.Uint64Var(&modulo, "sample-modulo", 1, "select one key digest bucket; 1 compares every key")
	flags.IntVar(&details, "max-details", 100, "maximum digest-only differences per table")
	fail := func(stage string) int {
		_, _ = fmt.Fprintln(errorOutput, "checkdata:", stage)
		return 2
	}
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || file == "" || timeout <= 0 || timeout > 24*time.Hour || modulo == 0 || details < 0 || details > 100 {
		return fail("invalid arguments; require -config and DSNs in environment variables")
	}
	validEnv := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	if !validEnv.MatchString(sourceEnv) || !validEnv.MatchString(targetEnv) {
		return fail("invalid DSN environment key")
	}
	input, err := os.Open(file)
	if err != nil {
		return fail("cannot read configuration")
	}
	defer func(cleanup func() error) { _ = cleanup() }(input.Close)
	body, err := io.ReadAll(io.LimitReader(input, 1024*1024+1))
	if err != nil || len(body) > 1024*1024 {
		return fail("configuration exceeds size limit or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var config datacheck.Config
	var trailing any
	if decoder.Decode(&config) != nil || decoder.Decode(&trailing) != io.EOF || config.Validate() != nil {
		return fail("invalid table mapping")
	}
	source, err := openDatabase(getenv(sourceEnv))
	if err != nil {
		return fail("invalid source DSN environment value")
	}
	defer func(cleanup func() error) { _ = cleanup() }(source.Close)
	target, err := openDatabase(getenv(targetEnv))
	if err != nil {
		return fail("invalid target DSN environment value")
	}
	defer func(cleanup func() error) { _ = cleanup() }(target.Close)
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	result, err := datacheck.New(source, target).Check(ctx, config, datacheck.Options{SampleModulo: modulo, MaxDetails: details})
	if err != nil {
		return fail("comparison failed; verify connectivity, table types and primary-key mapping")
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if encoder.Encode(result) != nil {
		return fail("cannot write result")
	}
	if !result.Equal {
		return 1
	}
	return 0
}
