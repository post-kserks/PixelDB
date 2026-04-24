package analyzer

import (
	"reflect"
	"testing"
)

func TestStandardAnalyzer(t *testing.T) {
	a := NewStandardAnalyzer()
	got := a.Analyze("The Warriors are running!")

	want := []AnalyzedToken{
		{Term: "warrior", Position: 1},
		{Term: "runn", Position: 3},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected tokens: got=%#v want=%#v", got, want)
	}
}
