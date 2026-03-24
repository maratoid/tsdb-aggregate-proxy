package remoteread

import (
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

func TestEscapeString(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"no special chars", "hello", "hello"},
		{"single quote", "it's", `it\'s`},
		{"backslash", `a\b`, `a\\b`},
		{"both", `a\'b`, `a\\\'b`},
		{"empty string", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, escapeString(tc.input))
		})
	}
}

func TestEscapeIdentifier(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"clean identifier", "metric_name", "metric_name"},
		{"alphanumeric", "abc123", "abc123"},
		{"dot", "my.metric", "my_metric"},
		{"dash", "foo-bar", "foo_bar"},
		{"space", "foo bar", "foo_bar"},
		{"mixed special chars", "ab.cd-ef gh", "ab_cd_ef_gh"},
		{"all invalid", "!@#", "___"},
		{"empty string", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, escapeIdentifier(tc.input))
		})
	}
}

func TestQueryId(t *testing.T) {
	t.Run("nil query returns empty string", func(t *testing.T) {
		assert.Equal(t, "", queryId(nil))
	})

	t.Run("non-nil query returns 7 hex chars", func(t *testing.T) {
		q := &prompb.Query{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
		}
		id := queryId(q)
		assert.Len(t, id, 7)
		assert.Regexp(t, `^[0-9a-f]{7}$`, id)
	})

	t.Run("same query is deterministic", func(t *testing.T) {
		q := &prompb.Query{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "cpu_usage"},
			},
		}
		assert.Equal(t, queryId(q), queryId(q))
	})

	t.Run("different queries produce different IDs", func(t *testing.T) {
		q1 := &prompb.Query{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "cpu_usage"},
			},
		}
		q2 := &prompb.Query{
			StartTimestampMs: 1000,
			EndTimestampMs:   2000,
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "memory_usage"},
			},
		}
		assert.NotEqual(t, queryId(q1), queryId(q2))
	})
}
