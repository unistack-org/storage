package crushmap

import (
	"errors"
	"strings"
	"unicode/utf8"
)

type stateFn func(*lex) stateFn

type tokType int

const (
	EOFRune rune = -1
)

const (
	itemError tokType = iota

	itemEOF

	itemComment

	itemTunableBeg
	itemTunableKey
	itemTunableVal
	itemTunableEnd

	itemDeviceBeg
	itemDeviceID
	itemDeviceName
	itemDeviceClass
	itemDeviceEnd

	itemTypeBeg
	itemTypeID
	itemTypeName
	itemTypeEnd

	itemBucketBeg
	itemBucketName
	itemBucketID
	itemBucketIDClass
	itemBucketAlg
	itemBucketHash
	itemBucketItemBeg
	itemBucketItemName
	itemBucketItemType
	itemBucketItemID
	itemBucketItemWeight
	itemBucketItemPos
	itemBucketItemEnd
	itemBucketEnd

	itemRuleBeg
	itemRuleName
	itemRuleID
	itemRuleRuleset
	itemRuleType
	itemRuleMinSize
	itemRuleMaxSize
	itemRuleStepBeg
	itemRuleStepSetChooseleafTries
	itemRuleStepSetChooseTries
	itemRuleStepTake
	itemRuleStepTakeType
	itemRuleStepChoose
	itemRuleStepChooseMSR
	itemRuleStepTakeClass
	itemRuleStepChooseMode
	itemRuleStepChooseNum
	itemRuleStepChooseType
	itemRuleStepSetGenericName
	itemRuleStepSetGeneric
	itemRuleStepEmit
	itemRuleStepEnd
	itemRuleEnd
)

type item struct {
	itype  tokType
	ivalue string
	iline  int
}

type lex struct {
	source     string
	start      int
	position   int
	line       int
	startState stateFn
	err        error
	items      chan item
	errHandler func(string)
	rewind     runeStack
	stack      []stateFn
}

func lexNew(src string, start stateFn) *lex {
	buffSize := len(src) / 2
	if buffSize <= 0 {
		buffSize = 1
	}

	return &lex{
		source:     src,
		startState: start,
		line:       1,
		rewind:     newRuneStack(),
		items:      make(chan item, buffSize),
		stack:      make([]stateFn, 0, 10),
	}
}

func (l *lex) lexStart() {
	go l.lexRun()
}

func (l *lex) lexStartSync() {
	l.lexRun()
}

func lexIsDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func (l *lex) lexCurrent() string {
	return l.source[l.start:l.position]
}

// lexKeyword returns the pending token with surrounding whitespace removed.
// States that terminate a token loop on a space leave that space inside
// lexCurrent(), so keyword comparisons must trim it.
func (l *lex) lexKeyword() string {
	return strings.TrimSpace(l.lexCurrent())
}

func (l *lex) lexEmit(t tokType) {
	itm := item{
		itype:  t,
		ivalue: l.lexCurrent(),
	}
	l.items <- itm
	l.start = l.position
	l.rewind.clear()
}

func (l *lex) lexEmitTrim(t tokType) {
	itm := item{
		itype:  t,
		ivalue: strings.TrimSpace(l.lexCurrent()),
	}
	l.items <- itm
	l.start = l.position
	l.rewind.clear()
}

func (l *lex) lexIgnore() {
	l.rewind.clear()
	l.start = l.position
}

func (l *lex) lexPeek() rune {
	r := l.lexNext()
	l.lexRewind()
	return r
}

func (l *lex) lexRewind() {
	r := l.rewind.pop()
	if r > EOFRune {
		size := utf8.RuneLen(r)
		l.position -= size
		if l.position < l.start {
			l.position = l.start
		}
	}
}

func (l *lex) lexNext() rune {
	var (
		r rune
		s int
	)
	str := l.source[l.position:]
	if len(str) == 0 {
		r, s = EOFRune, 0
	} else {
		r, s = utf8.DecodeRuneInString(str)
	}
	l.position += s
	l.rewind.push(r)

	return r
}

func (l *lex) lexPush(state stateFn) {
	l.stack = append(l.stack, state)
}

func (l *lex) lexPop() stateFn {
	if len(l.stack) == 0 {
		l.lexErr("BUG in lexer: no states to pop")
	}
	last := l.stack[len(l.stack)-1]
	l.stack = l.stack[0 : len(l.stack)-1]
	return last
}

func (l *lex) lexTake(chars string) {
	r := l.lexNext()
	for strings.ContainsRune(chars, r) {
		r = l.lexNext()
	}
	l.lexRewind() // last next wasn't a match
}

func (l *lex) lexNextToken() (*item, bool) {
	if itm, ok := <-l.items; ok {
		return &itm, false
	} else {
		return nil, true
	}
}

// lexErr records the first lexing error. States that call it return via
// lexPop; lexRun then stops the run and DecodeText surfaces the error.
func (l *lex) lexErr(e string) {
	if l.err == nil {
		l.err = errors.New(e)
	}
	if l.errHandler != nil {
		l.errHandler(e)
	}
}

func (l *lex) lexRun() {
	state := l.startState
	for state != nil {
		state = state(l)
		if l.err != nil {
			break
		}
	}
	close(l.items)
}
