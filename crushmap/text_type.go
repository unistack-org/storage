package crushmap

import (
	"errors"
	"strconv"
)

func typeState(l *lex) stateFn {
	l.lexEmit(itemTypeBeg)
	return typeIDState
}

func typeIDState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()
	for {
		r := l.lexPeek()
		if r == '\n' || r == ' ' || r == '#' || r == EOFRune {
			break
		}
		l.lexNext()
	}

	l.lexEmitTrim(itemTypeID)
	return typeNameState
}

func typeNameState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	for {
		r := l.lexPeek()
		if r == '\n' || r == ' ' || r == '#' || r == EOFRune {
			break
		}
		l.lexNext()
	}

	l.lexEmitTrim(itemTypeName)
	l.lexEmit(itemTypeEnd)
	return l.lexPop()
}

func (p *textParser) handleType() (*Type, error) {
	itype := &Type{ID: -1}

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF, itemTypeEnd:
			break Loop
		case itemComment:
			continue
		case itemTypeID:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			itype.ID = int32(id)
		case itemTypeName:
			itype.Name = tok.ivalue
		}
	}

	if itype.Name == "" {
		return nil, errors.New("invalid type")
	}

	return itype, nil
}
