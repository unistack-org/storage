package crushmap

func commentLineState(l *lex) stateFn {
loop:
	for {
		if r := l.lexPeek(); r == '\n' || r == EOFRune {
			if r == '\n' {
				l.lexNext()
			}
			break loop
		}
		l.lexNext()
	}
	l.lexEmitTrim(itemComment)
	return l.lexPop()
}
