package analyzer

// AnalyzedToken is a normalized token with position in the original token stream.
type AnalyzedToken struct {
	Term     string `json:"term"`
	Position int    `json:"position"`
}

// Tokenizer splits text into raw tokens.
type Tokenizer interface {
	Tokenize(text string) []AnalyzedToken
}

// TokenFilter transforms token stream.
type TokenFilter interface {
	Filter(tokens []AnalyzedToken) []AnalyzedToken
}

// Analyzer builds final analyzed token stream from text.
type Analyzer interface {
	Analyze(text string) []AnalyzedToken
}

// PipelineAnalyzer composes tokenizer and filters in order.
type PipelineAnalyzer struct {
	tokenizer Tokenizer
	filters   []TokenFilter
}

func NewPipelineAnalyzer(tokenizer Tokenizer, filters ...TokenFilter) *PipelineAnalyzer {
	return &PipelineAnalyzer{tokenizer: tokenizer, filters: filters}
}

func (a *PipelineAnalyzer) Analyze(text string) []AnalyzedToken {
	if a == nil || a.tokenizer == nil {
		return nil
	}

	tokens := a.tokenizer.Tokenize(text)
	for _, filter := range a.filters {
		if filter == nil {
			continue
		}
		tokens = filter.Filter(tokens)
	}
	return tokens
}
