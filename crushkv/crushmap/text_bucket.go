package crushmap

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func bucketState(l *lex) stateFn {
	l.lexEmit(itemBucketBeg)
	return bucketStartState
}

func bucketStartState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	r := l.lexPeek()
	switch r {
	case '{':
		l.lexNext()
		l.lexIgnore()
		return bucketIdentState
	case '#':
		l.lexErr(fmt.Sprintf("unexpected token %q", r))
		return l.lexPop()
	case EOFRune:
		l.lexErr("unexpected end of crush map")
		return nil
	}

	return bucketNameState
}

func bucketNameState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case '{':
			l.lexNext()
			l.lexIgnore()
			break loop
		case ' ':
			break loop
		case '\n', '#':
			l.lexErr(fmt.Sprintf("unexpected token %q", r))
			return l.lexPop()
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		default:
			l.lexNext()
		}
	}

	l.lexEmitTrim(itemBucketName)
	return bucketIdentState
}

func bucketIDState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop1:
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#', '\t':
			break loop1
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		}
		l.lexNext()
	}

	l.lexEmitTrim(itemBucketID)

	// The next keyword (`class`, `alg`, ...) is lexed by bucketIdentState;
	// consuming it here risks discarding it.
	return bucketIdentState
}

func bucketIDClassState(l *lex) stateFn {
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

	l.lexEmitTrim(itemBucketIDClass)

	return bucketIdentState
}

func bucketHashState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	l.lexTake("0123456789")
	l.lexEmitTrim(itemBucketHash)

	return bucketIdentState
}

func bucketAlgState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	for {
		r := l.lexPeek()
		if r == '\n' || r == ' ' || r == '#' || r == EOFRune {
			break
		}
		l.lexNext()
	}

	l.lexEmitTrim(itemBucketAlg)
	return bucketIdentState
}

// lexScanString advances past spaces/tabs and reads one string, stopping
// before ' ', '\n', '#', '}' or EOFRune. The runes stay inside the pending
// token (l.start:l.position), so callers must emit or rewind before scanning
// again.
func lexScanString(l *lex) (string, bool) {
	l.lexTake(" \t")
	var b strings.Builder
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#', '}', EOFRune:
			return b.String(), b.Len() > 0
		}
		l.lexNext()
		b.WriteRune(r)
	}
}

// bucketItemState reads the token(s) after `item`. Ceph accepts two forms:
//
//	item <name>                // current form
//	item <type> <name>         // deprecated positional form
//
// We scan the first string, then peek a second space-delimited string on the
// same line. If a second string follows (and is not a keyword like `id`), the
// first token is the deprecated type and the second is the name; otherwise the
// single token is the name. Name resolution drives bucket placement.
func bucketItemState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()
	mark := l.position

	first, ok := lexScanString(l)
	if !ok {
		r := l.lexPeek()
		switch r {
		case '\n', '#':
			l.lexErr(fmt.Sprintf("unexpected token %q", r))
			return l.lexPop()
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		}
	}

	scan := l.position
	second, hasSecond := lexScanString(l)
	if hasSecond && !isItemKeyword(second) {
		// Positional form: first is the (deprecated) type, second the name.
		// Trim explicitly: the pending slices carry the separators between
		// the two strings.
		end := l.position
		l.position = mark + len(first)
		l.lexEmitTrim(itemBucketItemType)
		l.start = scan
		l.position = end
		l.lexEmitTrim(itemBucketItemName)
		return bucketItemIdentState
	}

	// Single-token form: rewind to the end of the first string (dropping the
	// scanned-ahead keyword), then emit it as the name.
	for l.position > mark+len(first) {
		l.lexRewind()
	}
	l.lexEmitTrim(itemBucketItemName)
	return bucketItemIdentState
}

// isItemKeyword reports whether tok terminates an item name, i.e. is one of
// the keywords accepted in bucketItemIdentState.
func isItemKeyword(tok string) bool {
	switch tok {
	case "id", "weight", "pos", "}":
		return true
	}
	return false
}

func bucketItemIdentState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#':
			break loop
		case '}':
			// One-line bucket: `... item osd.1 weight 1.000 }`. End the
			// item here and leave the brace for bucketIdentState, which
			// closes the bucket.
			l.lexEmit(itemBucketItemEnd)
			return bucketIdentState
		case EOFRune:
			l.lexErr("unexpected end of crush map")
			return nil
		default:
			l.lexNext()
		}
	}

	switch l.lexKeyword() {
	case "weight":
		l.lexIgnore()
		return bucketItemWeightState
	case "pos":
		l.lexIgnore()
		return bucketItemPosState
	case "id":
		// `item host h1 id -1 weight 1.000`: Ceph allows an explicit id on
		// item lines (typical for nested-bucket items in maps generated with
		// ids). The value may be negative.
		l.lexIgnore()
		lexSignedInt(l)
		l.lexEmitTrim(itemBucketItemID)
		return bucketItemIdentState
	}

	// Not an item attribute: the pending text begins the next construct
	// (the next `item ...`, a stray token before the bucket's closing
	// brace...). End the item and rewind so bucketIdentState re-reads it.
	mark := l.start
	l.lexEmit(itemBucketItemEnd)
	l.position = mark
	l.start = mark
	l.rewind.clear()
	return bucketIdentState
}

func bucketItemWeightState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	l.lexTake(".0123456789")
	l.lexEmitTrim(itemBucketItemWeight)

	return bucketItemIdentState
}

func bucketItemPosState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

	l.lexTake("0123456789")
	l.lexEmitTrim(itemBucketItemPos)

	return bucketItemIdentState
}

func bucketIdentState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case ' ':
			break loop
		case '{':
			// A bucket written on one line (`root r { id -2 ... }`) has a
			// space after the brace; the brace must be consumed here, not
			// left for the keyword switch below.
			l.lexNext()
			l.lexIgnore()
			return bucketIdentState
		case '#':
			l.lexNext()
			l.lexIgnore()
			l.lexPush(bucketIdentState)
			return commentLineState
		case '}':
			l.lexNext()
			l.lexIgnore()
			l.lexEmit(itemBucketEnd)
			return l.lexPop()
		case '\n':
			l.lexNext()
			l.lexIgnore()
			return bucketIdentState
		case EOFRune:
			l.lexErr("unexpected end of crush map (unterminated bucket)")
			return nil
		default:
			l.lexNext()
		}
	}

	switch l.lexKeyword() {
	case "id":
		l.lexIgnore()
		return bucketIDState
	case "class":
		// `id <n> class <c>`: bucketIDState leaves us here right after the
		// id value, with `class` as the next token.
		l.lexIgnore()
		return bucketIDClassState
	case "alg":
		l.lexIgnore()
		return bucketAlgState
	case "hash":
		l.lexIgnore()
		return bucketHashState
	case "item":
		l.lexIgnore()
		l.lexEmit(itemBucketItemBeg)
		return bucketItemState
	}

	// Unknown keyword inside a bucket: skip it and keep parsing the bucket
	// body (matches the lenient keyword handling of the states above).
	l.lexIgnore()
	return bucketIdentState
}

func (p *textParser) handleBucket(itype string) (*Bucket, error) {
	ibucket := &Bucket{TypeName: itype}

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF, itemBucketEnd:
			break Loop
		case itemComment:
			continue
		case itemBucketName:
			ibucket.Name = tok.ivalue
		case itemBucketIDClass:
			ibucket.IDClass = tok.ivalue
		case itemBucketID:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			ibucket.ID = int32(id)
		case itemBucketAlg:
			ibucket.Alg = tok.ivalue
		case itemBucketHash:
			if tok.ivalue == "0" {
				ibucket.Hash = "rjenkins1"
			} else {
				return nil, errors.New("invalid bucket hash")
			}
		case itemBucketItemBeg:
			item, posSet, err := p.handleBucketItem()
			if err != nil {
				return nil, err
			}
			if !posSet {
				// Ceph's text format assigns pos implicitly: the position
				// of the item line inside the bucket. An explicit `pos N`
				// attribute (handled below) wins.
				item.Pos = len(ibucket.Items)
			}
			ibucket.Items = append(ibucket.Items, item)
		}
	}

	return ibucket, nil
}

func (p *textParser) handleBucketItem() (*Item, bool, error) {
	item := &Item{}
	posSet := false

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF, itemBucketItemEnd:
			break Loop
		case itemComment:
			continue
		case itemBucketItemName:
			item.Name = tok.ivalue
		case itemBucketItemID:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, false, err
			}
			item.ID = int32(id)
		case itemBucketItemWeight:
			id, err := strconv.ParseFloat(tok.ivalue, 32)
			if err != nil {
				return nil, false, err
			}
			item.Weight = float32(id)
		case itemBucketItemPos:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, false, err
			}
			item.Pos = id
			posSet = true
		}
	}
	return item, posSet, nil
}
