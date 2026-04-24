package analyzer

import "unicode"

// WhitespaceTokenizer splits by any non-letter/non-digit rune.
type WhitespaceTokenizer struct{}

func (WhitespaceTokenizer) Tokenize(text string) []AnalyzedToken {
	if text == "" {
		return nil
	}

	runes := []rune(text)
	tokens := make([]AnalyzedToken, 0, 8)
	position := 0

	start := -1
	for i, ch := range runes {
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			if start == -1 {
				start = i
			}
			continue
		}
		if start != -1 {
			tokens = append(tokens, AnalyzedToken{
				Term:     string(runes[start:i]),
				Position: position,
			})
			position++
			start = -1
		}
	}

	if start != -1 {
		tokens = append(tokens, AnalyzedToken{
			Term:     string(runes[start:]),
			Position: position,
		})
	}

	return tokens
}
