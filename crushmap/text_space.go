package crushmap

import "fmt"

func spaceState(l *lex) stateFn {
	r := l.lexNext()

	if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
		l.lexErr(fmt.Sprintf("unexpected token %q", r))
		return nil
	}

	l.lexTake(" \t")
	l.lexIgnore()
	return l.lexPop()
}
