package schemamig

import (
	"errors"
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

// ValidateDDLCharacterSets checks explicit declarations in already validated
// migration SQL against actual target metadata. The lexer excludes comments and
// string contents, including routine bodies containing example SQL text.
func ValidateDDLCharacterSets(ddl string, characterSets, collations []string) error {
	sets, sorts := map[string]bool{}, map[string]bool{}
	for _, name := range characterSets {
		sets[strings.ToLower(name)] = true
	}
	for _, name := range collations {
		sorts[strings.ToLower(name)] = true
	}
	if len(sets) == 0 || len(sorts) == 0 || len(sets) > 1000 || len(sorts) > 1000 {
		return errors.New("target character set capabilities unavailable")
	}
	lexer := mysql.NewMySQLLexer(antlr.NewInputStream(ddl))
	errs := newRoutineErrorListener()
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errs)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	stream.Fill()
	if len(errs.errs) != 0 {
		return errors.New("invalid character set declaration")
	}
	var tokens []string
	for _, token := range stream.GetAllTokens() {
		if token.GetChannel() == antlr.TokenDefaultChannel && token.GetTokenType() != antlr.TokenEOF {
			tokens = append(tokens, token.GetText())
		}
	}
	for i := 0; i < len(tokens); i++ {
		word := strings.ToUpper(tokens[i])
		var allowed map[string]bool
		switch {
		case word == "CHARSET":
			allowed = sets
		case word == "COLLATE":
			allowed = sorts
		case word == "CHARACTER" && i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "SET"):
			allowed = sets
			i++
		case strings.HasPrefix(word, "_") && i+1 < len(tokens) && strings.HasPrefix(tokens[i+1], "'"):
			if !sets[strings.ToLower(tokens[i][1:])] {
				return fmt.Errorf("unsupported character set %s", tokens[i][1:])
			}
			continue
		default:
			continue
		}
		i++
		if i < len(tokens) && tokens[i] == "=" {
			i++
		}
		if i >= len(tokens) {
			return errors.New("missing character set or collation name")
		}
		name := strings.Trim(tokens[i], "`'\"")
		if !allowed[strings.ToLower(name)] {
			return fmt.Errorf("unsupported character set or collation %s", name)
		}
	}
	return nil
}
