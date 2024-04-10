package ql

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestTranslateDialect(t *testing.T) {
	tr := NewTranslator(LogsCondTranslateDialect)
	var sb strings.Builder
	e, err := parser.ParseString("", "tag('abc') = tag(\"def\") and (logID = '123' or logID in ['g', '88']) or tag('f3') like \"aaa%\"")
	assert.Nil(t, err)
	assert.Nil(t, tr.Expression2Sql(&sb, e))
	assert.Equal(t, "tags ->> 'abc' = tags ->> 'def' AND (id = '123' OR id IN ('g', '88')) OR tags ->> 'f3' LIKE 'aaa%'", sb.String())
}
