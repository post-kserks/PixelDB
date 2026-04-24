package analyzer

var defaultStopWords = []string{
	"a", "an", "and", "are", "as", "at", "be", "been", "but", "by",
	"for", "from", "had", "has", "have", "he", "her", "his", "i", "in",
	"is", "it", "its", "of", "on", "or", "she", "that", "the", "their",
	"there", "they", "this", "to", "was", "were", "will", "with", "you",
}

// NewStandardAnalyzer creates a default text analysis pipeline.
func NewStandardAnalyzer() Analyzer {
	return NewPipelineAnalyzer(
		WhitespaceTokenizer{},
		LowercaseFilter{},
		NewStopWordFilter(defaultStopWords),
		SuffixStemmer{},
	)
}
