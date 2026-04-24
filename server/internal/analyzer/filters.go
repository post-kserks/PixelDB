package analyzer

import "strings"

// LowercaseFilter normalizes all token terms to lower case.
type LowercaseFilter struct{}

func (LowercaseFilter) Filter(tokens []AnalyzedToken) []AnalyzedToken {
	if len(tokens) == 0 {
		return nil
	}

	result := make([]AnalyzedToken, len(tokens))
	for i, tok := range tokens {
		result[i] = AnalyzedToken{
			Term:     strings.ToLower(tok.Term),
			Position: tok.Position,
		}
	}
	return result
}

// StopWordFilter removes common stop words.
type StopWordFilter struct {
	stopWords map[string]struct{}
}

func NewStopWordFilter(words []string) StopWordFilter {
	m := make(map[string]struct{}, len(words))
	for _, word := range words {
		m[strings.ToLower(word)] = struct{}{}
	}
	return StopWordFilter{stopWords: m}
}

func (f StopWordFilter) Filter(tokens []AnalyzedToken) []AnalyzedToken {
	if len(tokens) == 0 {
		return nil
	}
	if len(f.stopWords) == 0 {
		copied := make([]AnalyzedToken, len(tokens))
		copy(copied, tokens)
		return copied
	}

	result := make([]AnalyzedToken, 0, len(tokens))
	for _, tok := range tokens {
		if _, isStopWord := f.stopWords[tok.Term]; isStopWord {
			continue
		}
		result = append(result, tok)
	}
	return result
}

// SuffixStemmer strips common suffixes using a light stemmer heuristic.
type SuffixStemmer struct{}

func (SuffixStemmer) Filter(tokens []AnalyzedToken) []AnalyzedToken {
	if len(tokens) == 0 {
		return nil
	}

	result := make([]AnalyzedToken, len(tokens))
	for i, tok := range tokens {
		result[i] = AnalyzedToken{
			Term:     stemTerm(tok.Term),
			Position: tok.Position,
		}
	}
	return result
}

func stemTerm(term string) string {
	if len([]rune(term)) < 4 {
		return term
	}

	suffixes := []string{
		"ments", "ation", "ition", "ingly", "ingly",
		"ment", "tion", "ness", "edly", "ing", "ies", "ied", "ed", "es", "s",
	}
	for _, suffix := range suffixes {
		if strings.HasSuffix(term, suffix) {
			candidate := strings.TrimSuffix(term, suffix)
			if len([]rune(candidate)) >= 3 {
				return candidate
			}
		}
	}
	return term
}
