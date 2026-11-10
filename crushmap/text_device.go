package crushmap

import (
	"errors"
	"fmt"
	"strconv"
)

func deviceState(l *lex) stateFn {
	l.lexIgnore()
	l.lexEmit(itemDeviceBeg)

	return deviceIDState
}

func deviceIDState(l *lex) stateFn {
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

	l.lexEmitTrim(itemDeviceID)
	return deviceNameState
}

func deviceNameState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#', '\t':
			break loop
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		default:
			l.lexNext()
		}
	}

	l.lexEmitTrim(itemDeviceName)
	return deviceIdentState
}

func deviceIdentState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case '\n', '#':
			l.lexEmit(itemDeviceEnd)
			return l.lexPop()
		case EOFRune:
			if l.lexCurrent() != "" {
				// trailing keyword (e.g. "class") with no value
				l.lexErr("unexpected end of crush map")
				return nil
			}
			l.lexEmit(itemDeviceEnd)
			return l.lexPop()
		case ' ':
			break loop
		default:
			l.lexNext()
		}
	}

	switch l.lexKeyword() {
	case "class":
		l.lexIgnore()
		return deviceClassState
	}

	return l.lexPop()
}

func deviceClassState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case '\n', '#', ' ':
			break loop
		case EOFRune:
			if l.lexCurrent() == "" {
				l.lexErr("unexpected end of crush map")
				return nil
			}
			l.lexEmitTrim(itemDeviceClass)
			l.lexEmit(itemDeviceEnd)
			return l.lexPop()
		default:
			l.lexNext()
		}
	}

	l.lexEmitTrim(itemDeviceClass)
	return deviceIdentState
}

func (p *textParser) handleDevice() (*Device, error) {
	idevice := &Device{ID: -1}

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF, itemDeviceEnd:
			break Loop
		case itemComment:
			continue
		case itemDeviceID:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			idevice.ID = int32(id)
		case itemDeviceName:
			idevice.Name = tok.ivalue
		case itemDeviceClass:
			idevice.Class = tok.ivalue
		}
	}

	if idevice.Name == "" {
		return nil, errors.New("invalid device")
	}

	return idevice, nil
}
