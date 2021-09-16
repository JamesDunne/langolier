package main

import (
	"strings"
	"unicode"
)

func nString(s string) *string {
	p := new(string)
	*p = s
	return p
}

func nTrimRight(s *string) *string {
	if s == nil {
		return nil
	}
	p := new(string)
	*p = strings.TrimRightFunc(*s, unicode.IsSpace)
	return p
}

func nIntBool(v *int, conv func(int) bool) *bool {
	if v == nil {
		return nil
	}
	b := new(bool)
	*b = conv(*v)
	return b
}

func nBool(v bool) *bool {
	p := new(bool)
	*p = v
	return p
}

func nInt(v int) *int {
	p := new(int)
	*p = v
	return p
}
