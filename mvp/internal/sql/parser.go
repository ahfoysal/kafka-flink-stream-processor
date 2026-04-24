// Package sql is a tiny streaming-SQL frontend.
//
// Grammar (subset):
//
//   SELECT <key_col>, COUNT(*)
//   FROM <source_topic>
//   GROUP BY <key_col>,
//            TUMBLE(<ts_col>, INTERVAL '<n>' <unit>)
//   EMIT ON WATERMARK
//   [INTO <sink_topic>]
//
//   <unit> ::= SECOND | MILLISECOND
//
// The parser is intentionally hand-rolled — no ANTLR, no regexp soup. We
// produce an IR (Query) which the planner lowers into a StreamGraph.
//
// Lexing is whitespace-separated with a couple of special cases for
// punctuation and quoted interval literals.
package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Query is the parsed SQL statement.
type Query struct {
	KeyCol      string
	SourceTopic string
	TsCol       string
	WindowSize  time.Duration
	SinkTopic   string // "" if INTO omitted
}

// Parse is the single entrypoint.
func Parse(src string) (*Query, error) {
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q := &Query{}

	if err := p.expectKW("SELECT"); err != nil {
		return nil, err
	}
	col, err := p.ident()
	if err != nil {
		return nil, err
	}
	q.KeyCol = col
	if err := p.expect(","); err != nil {
		return nil, err
	}
	if err := p.expectKW("COUNT"); err != nil {
		return nil, err
	}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	if err := p.expect("*"); err != nil {
		return nil, err
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if err := p.expectKW("FROM"); err != nil {
		return nil, err
	}
	topic, err := p.ident()
	if err != nil {
		return nil, err
	}
	q.SourceTopic = topic
	if err := p.expectKW("GROUP"); err != nil {
		return nil, err
	}
	if err := p.expectKW("BY"); err != nil {
		return nil, err
	}
	grpKey, err := p.ident()
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(grpKey, q.KeyCol) {
		return nil, fmt.Errorf("GROUP BY %q must match SELECT key column %q", grpKey, q.KeyCol)
	}
	if err := p.expect(","); err != nil {
		return nil, err
	}
	if err := p.expectKW("TUMBLE"); err != nil {
		return nil, err
	}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	tsCol, err := p.ident()
	if err != nil {
		return nil, err
	}
	q.TsCol = tsCol
	if err := p.expect(","); err != nil {
		return nil, err
	}
	if err := p.expectKW("INTERVAL"); err != nil {
		return nil, err
	}
	lit, err := p.consumeStringLiteral()
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(lit)
	if err != nil {
		return nil, fmt.Errorf("interval literal must be integer, got %q", lit)
	}
	unit, err := p.ident()
	if err != nil {
		return nil, err
	}
	dur, err := unitToDuration(n, unit)
	if err != nil {
		return nil, err
	}
	q.WindowSize = dur
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if err := p.expectKW("EMIT"); err != nil {
		return nil, err
	}
	if err := p.expectKW("ON"); err != nil {
		return nil, err
	}
	if err := p.expectKW("WATERMARK"); err != nil {
		return nil, err
	}

	// Optional INTO <topic>
	if p.peekKW("INTO") {
		p.next()
		topic, err := p.ident()
		if err != nil {
			return nil, err
		}
		q.SinkTopic = topic
	}

	// Optional trailing semicolon.
	if p.peek(";") {
		p.next()
	}
	if !p.eof() {
		return nil, fmt.Errorf("unexpected trailing tokens starting at %q", p.current())
	}
	return q, nil
}

// ----------------------------------------------------------------------------
// lexer
// ----------------------------------------------------------------------------

type tok struct {
	v      string
	isStr  bool
}

func tokenize(src string) ([]tok, error) {
	var out []tok
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'':
			// string literal: 'nnn'
			j := i + 1
			for j < len(src) && src[j] != '\'' {
				j++
			}
			if j >= len(src) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			out = append(out, tok{v: src[i+1 : j], isStr: true})
			i = j + 1
		case c == '(' || c == ')' || c == ',' || c == ';' || c == '*':
			out = append(out, tok{v: string(c)})
			i++
		default:
			// identifier / keyword / number
			j := i
			for j < len(src) {
				d := src[j]
				if d == ' ' || d == '\t' || d == '\n' || d == '\r' ||
					d == '(' || d == ')' || d == ',' || d == ';' || d == '*' || d == '\'' {
					break
				}
				j++
			}
			out = append(out, tok{v: src[i:j]})
			i = j
		}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// parser
// ----------------------------------------------------------------------------

type parser struct {
	toks []tok
	pos  int
}

func (p *parser) eof() bool           { return p.pos >= len(p.toks) }
func (p *parser) current() string     { if p.eof() { return "<eof>" }; return p.toks[p.pos].v }
func (p *parser) next() tok           { t := p.toks[p.pos]; p.pos++; return t }
func (p *parser) peek(s string) bool  { return !p.eof() && p.toks[p.pos].v == s }
func (p *parser) peekKW(s string) bool {
	if p.eof() {
		return false
	}
	t := p.toks[p.pos]
	return !t.isStr && strings.EqualFold(t.v, s)
}

func (p *parser) expect(s string) error {
	if p.eof() || p.toks[p.pos].v != s {
		return fmt.Errorf("expected %q, got %q", s, p.current())
	}
	p.pos++
	return nil
}
func (p *parser) expectKW(s string) error {
	if !p.peekKW(s) {
		return fmt.Errorf("expected keyword %q, got %q", s, p.current())
	}
	p.pos++
	return nil
}

func (p *parser) ident() (string, error) {
	if p.eof() {
		return "", fmt.Errorf("expected identifier, got EOF")
	}
	t := p.toks[p.pos]
	if t.isStr {
		return "", fmt.Errorf("expected identifier, got string literal %q", t.v)
	}
	if !isIdent(t.v) {
		return "", fmt.Errorf("expected identifier, got %q", t.v)
	}
	p.pos++
	return t.v, nil
}

func (p *parser) consumeStringLiteral() (string, error) {
	if p.eof() || !p.toks[p.pos].isStr {
		return "", fmt.Errorf("expected string literal, got %q", p.current())
	}
	t := p.toks[p.pos]
	p.pos++
	return t.v, nil
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= 'A' && c <= 'Z' {
			continue
		}
		if c == '_' {
			continue
		}
		if i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func unitToDuration(n int, unit string) (time.Duration, error) {
	switch strings.ToUpper(unit) {
	case "SECOND", "SECONDS":
		return time.Duration(n) * time.Second, nil
	case "MILLISECOND", "MILLISECONDS":
		return time.Duration(n) * time.Millisecond, nil
	case "MINUTE", "MINUTES":
		return time.Duration(n) * time.Minute, nil
	}
	return 0, fmt.Errorf("unsupported INTERVAL unit %q (expected SECOND|MILLISECOND|MINUTE)", unit)
}
