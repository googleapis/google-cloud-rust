package language

import (
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/src/genclient"
	"github.com/googleapis/google-cloud-rust/generator/src/genclient/language/internal/golang"
	"github.com/googleapis/google-cloud-rust/generator/src/genclient/language/internal/rust"
)

// language is a supported programming language of the generator.
type language int

const (
	undefinedLanguage language = iota
	rustLanguage
	goLanguage
)

func strToLanguage(key string) language {
	strToLangMap := map[string]language{
		"rust": rustLanguage,
		"go":   goLanguage,
	}

	key = strings.ToLower(strings.TrimSpace(key))
	v, ok := strToLangMap[key]
	if !ok {
		return undefinedLanguage
	}
	return v
}

// NewCodec returns a new language codec based on the given language.
func NewCodec(language string) (*Codec, error) {
	var codec *Codec
	switch strToLanguage(language) {
	case rustLanguage:
		codec = &Codec{
			LanguageCodec: rust.NewCodec(),
		}
	case goLanguage:
		codec = &Codec{
			LanguageCodec: golang.NewCodec(),
		}
	default:
		// undefinedLanguage
		return nil, fmt.Errorf("unknown language: %s", language)
	}
	return codec, nil
}

// Codec is an adapter used to transform values into language idiomatic
// representations.
type Codec struct {
	genclient.LanguageCodec
}
