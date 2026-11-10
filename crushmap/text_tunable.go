package crushmap

import (
	"errors"
	"fmt"
	"strconv"
)

func tunableState(l *lex) stateFn {
	l.lexIgnore()

	if r := l.lexPeek(); r != ' ' {
		l.lexErr(fmt.Sprintf("unexpected token %q", r))
		return l.lexPop()
	}

	l.lexEmit(itemTunableBeg)

	return tunableKeyState
}

func tunableKeyState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case '\n', '#':
			l.lexErr(fmt.Sprintf("unexpected token %q", r))
			return l.lexPop()
		case ' ':
			break loop
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		default:
			l.lexNext()
		}
	}

	l.lexEmitTrim(itemTunableKey)

	return tunableValState
}

func tunableValState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case '\n', '#', ' ':
			break loop
		case EOFRune:
			break loop
		default:
			l.lexNext()
		}
	}

	l.lexEmitTrim(itemTunableVal)

	l.lexEmit(itemTunableEnd)

	return l.lexPop()
}

func (p *textParser) handleTunable() (string, interface{}, error) {
	var key string
	var val interface{}

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}

		switch tok.itype {
		case itemEOF, itemTunableEnd:
			break Loop
		case itemComment:
			continue
		case itemTunableKey:
			key = tok.ivalue
		case itemTunableVal:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				val = tok.ivalue
			} else {
				val = id
			}
		}
	}

	if key == "" {
		return "", nil, errors.New("invalid tunable")
	}

	return key, val, nil
}
