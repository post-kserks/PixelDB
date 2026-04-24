package lexer

import "testing"

func TestLexerBasic(t *testing.T) {
	sql := "SeLeCt id, name FROM heroes WHERE level >= -3 AND alive = TRUE;"
	l := New(sql)

	want := []TokenType{
		TOKEN_SELECT,
		TOKEN_IDENT,
		TOKEN_COMMA,
		TOKEN_IDENT,
		TOKEN_FROM,
		TOKEN_IDENT,
		TOKEN_WHERE,
		TOKEN_IDENT,
		TOKEN_GTE,
		TOKEN_INT_LIT,
		TOKEN_AND,
		TOKEN_IDENT,
		TOKEN_EQ,
		TOKEN_TRUE,
		TOKEN_SEMICOLON,
		TOKEN_EOF,
	}

	for i, tokenType := range want {
		tok := l.NextToken()
		if tok.Type != tokenType {
			t.Fatalf("token[%d]: expected %s, got %s (%q)", i, tokenType, tok.Type, tok.Literal)
		}
	}
}

func TestLexerStringLiteral(t *testing.T) {
	sql := "INSERT INTO heroes VALUES ('Legolas\\'s bow', 9);"
	l := New(sql)

	for {
		tok := l.NextToken()
		if tok.Type == TOKEN_STRING_LIT {
			if tok.Literal != "Legolas's bow" {
				t.Fatalf("unexpected string literal: %q", tok.Literal)
			}
			return
		}
		if tok.Type == TOKEN_EOF {
			t.Fatal("string literal token not found")
		}
	}
}

func TestLexerNewKeywords(t *testing.T) {
	sql := "SELECT name, _score FROM heroes WHERE MATCH(bio, 'dragon') ORDER BY _score DESC LIMIT 5 OFFSET 2;"
	l := New(sql)

	want := []TokenType{
		TOKEN_SELECT,
		TOKEN_IDENT,
		TOKEN_COMMA,
		TOKEN_IDENT,
		TOKEN_FROM,
		TOKEN_IDENT,
		TOKEN_WHERE,
		TOKEN_MATCH,
		TOKEN_LPAREN,
		TOKEN_IDENT,
		TOKEN_COMMA,
		TOKEN_STRING_LIT,
		TOKEN_RPAREN,
		TOKEN_ORDER,
		TOKEN_BY,
		TOKEN_IDENT,
		TOKEN_DESC,
		TOKEN_LIMIT,
		TOKEN_INT_LIT,
		TOKEN_OFFSET,
		TOKEN_INT_LIT,
		TOKEN_SEMICOLON,
		TOKEN_EOF,
	}

	for i, tokenType := range want {
		tok := l.NextToken()
		if tok.Type != tokenType {
			t.Fatalf("token[%d]: expected %s, got %s (%q)", i, tokenType, tok.Type, tok.Literal)
		}
	}
}
