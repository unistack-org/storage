package crushmap

import (
	"errors"
	"fmt"
	"strconv"
)

func ruleState(l *lex) stateFn {
	l.lexEmit(itemRuleBeg)

	l.lexTake(" \t")
	l.lexIgnore()

	r := l.lexPeek()
	switch r {
	case '{':
		l.lexNext()
		l.lexIgnore()
		return ruleIdentState
	case '#':
		l.lexErr(fmt.Sprintf("unexpected token %q", r))
		return l.lexPop()
	}

	return ruleNameState
}

func ruleIdentState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case ' ':
			break loop
		case '{':
			// See the same fix in bucketIdentState: consume a brace that is
			// followed by a space on a single-line rule.
			l.lexNext()
			l.lexIgnore()
			return ruleIdentState
		case '#':
			l.lexNext()
			l.lexIgnore()
			l.lexPush(ruleIdentState)
			return commentLineState
		case '}':
			l.lexNext()
			l.lexIgnore()
			l.lexEmit(itemRuleEnd)
			return l.lexPop()
		case '\n':
			l.lexNext()
			l.lexIgnore()
			return ruleIdentState
		case EOFRune:
			l.lexErr("unexpected end of crush map (unterminated rule)")
			return nil
		default:
			l.lexNext()
		}
	}

	switch l.lexKeyword() {
	case "id", "ruleset":
		// Ceph grammar: (id | ruleset) >> posint; both set the rule id.
		l.lexIgnore()
		lexSignedInt(l)
		l.lexEmitTrim(itemRuleID)
		return ruleIdentState
	case "min_size":
		l.lexIgnore()
		lexSignedInt(l)
		l.lexEmitTrim(itemRuleMinSize)
		return ruleIdentState
	case "max_size":
		l.lexIgnore()
		lexSignedInt(l)
		l.lexEmitTrim(itemRuleMaxSize)
		return ruleIdentState
	case "type":
		l.lexIgnore()
		lexTokenToEmit(l)
		l.lexEmitTrim(itemRuleType)
		return ruleIdentState
	case "step":
		l.lexIgnore()
		l.lexEmit(itemRuleStepBeg)
		return ruleStepIdentState
	}
	return l.lexPop()
}

// lexSignedInt consumes an optional '-' followed by digits. Ceph's grammar
// uses integer (not posint) for the choose num: `chooseleaf firstn -1 type
// host` is the standard replicated rule, so the minus sign must parse.
func lexSignedInt(l *lex) {
	l.lexTake(" \t")
	l.lexIgnore()
	if l.lexPeek() == '-' {
		l.lexNext()
	}
	l.lexTake("0123456789")
}

// lexTokenToEmit consumes whitespace plus one bare token up to whitespace,
// newline, '#', EOFRune or '}'.
func lexTokenToEmit(l *lex) {
	l.lexTake(" \t")
	l.lexIgnore()
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#', '\t', '}', EOFRune:
			return
		default:
			l.lexNext()
		}
	}
}

func ruleStepIdentState(l *lex) stateFn {
	l.lexTake(" \t")
	l.lexIgnore()

loop:
	for {
		r := l.lexPeek()
		switch r {
		case ' ', '\n', '#', '\t':
			break loop
		case EOFRune:
			break loop
		default:
			l.lexNext()
		}
	}

	switch l.lexKeyword() {
	case "set_chooseleaf_tries", "set_choose_tries",
		"set_choose_local_tries", "set_choose_local_fallback_tries",
		"set_chooseleaf_vary_r", "set_chooseleaf_stable",
		"set_msr_descents", "set_msr_collision_tries":
		name := l.lexKeyword()
		l.lexIgnore()
		l.items <- item{itype: itemRuleStepSetGenericName, ivalue: name}
		lexSignedInt(l)
		l.lexEmitTrim(itemRuleStepSetGeneric)
		l.lexEmit(itemRuleStepEnd)
		return ruleIdentState
	case "take":
		l.lexIgnore()
		l.lexEmit(itemRuleStepTake)
		return ruleStepTake
	case "choose", "chooseleaf":
		l.lexEmit(itemRuleStepChoose)
		return ruleStepChoose
	case "choosemsr":
		l.lexEmit(itemRuleStepChooseMSR)
		// choosemsr <int> type <name>
		lexSignedInt(l)
		l.lexEmitTrim(itemRuleStepChooseNum)
		mark := l.position
		lexTokenToEmit(l)
		if l.lexCurrent() == "type" {
			l.lexIgnore()
			lexTokenToEmit(l)
			l.lexEmit(itemRuleStepChooseType)
		} else {
			l.position = mark
			l.start = mark
			l.rewind.clear()
		}
		l.lexEmit(itemRuleStepEnd)
		return ruleIdentState
	case "emit":
		l.lexEmit(itemRuleStepEmit)
		l.lexEmit(itemRuleStepEnd)
		return ruleIdentState
	}

	return ruleIdentState
}

// ruleStepChoose parses: (indep|firstn) <int> [type <name>]
func ruleStepChoose(l *lex) stateFn {
	lexTokenToEmit(l)
	mode := l.lexCurrent()
	switch mode {
	case "firstn", "indep":
		l.lexIgnore()
		l.items <- item{itype: itemRuleStepChooseMode, ivalue: mode}

		lexSignedInt(l)
		l.lexEmitTrim(itemRuleStepChooseNum)

		mark := l.position
		lexTokenToEmit(l)
		if l.lexCurrent() == "type" {
			l.lexIgnore()
			lexTokenToEmit(l)
			l.lexEmit(itemRuleStepChooseType)
		} else {
			l.position = mark
			l.start = mark
			l.rewind.clear()
		}
		l.lexEmit(itemRuleStepEnd)
		return ruleIdentState
	case "type":
		// legacy crushkv syntax: "choose type <name>"
		l.lexIgnore()
		lexTokenToEmit(l)
		l.lexEmit(itemRuleStepChooseType)
		return ruleStepChoose
	}

	l.lexEmit(itemRuleStepEnd)
	return ruleIdentState
}

func ruleStepTake(l *lex) stateFn {
	lexTokenToEmit(l)
	l.lexEmit(itemRuleStepTakeType)

	mark := l.position
	lexTokenToEmit(l)
	if l.lexCurrent() == "class" {
		l.lexIgnore()
		lexTokenToEmit(l)
		l.lexEmit(itemRuleStepTakeClass)
	} else {
		l.position = mark
		l.start = mark
		l.rewind.clear()
	}

	l.lexEmit(itemRuleStepEnd)
	return ruleIdentState
}

func ruleNameState(l *lex) stateFn {
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
	l.lexEmit(itemRuleName)
	return ruleIdentState
}

func (p *textParser) handleRule() (*Rule, error) {
	irule := &Rule{}
	ended := false

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF:
			break Loop
		case itemRuleEnd:
			ended = true
			break Loop
		case itemComment:
			continue
		case itemRuleMinSize:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			irule.MinSize = uint8(id)
		case itemRuleMaxSize:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			irule.MaxSize = uint8(id)
		case itemRuleStepBeg:
			istep, err := p.handleRuleStep()
			if err != nil {
				return nil, err
			}
			irule.Steps = append(irule.Steps, istep)
		case itemRuleName:
			irule.Name = tok.ivalue
		case itemRuleType:
			switch tok.ivalue {
			case "replicated":
				irule.Type = ReplicatedPG
			case "erasure":
				irule.Type = ErasurePG
			case "msr_firstn":
				irule.Type = 4
			case "msr_indep":
				irule.Type = 5
			default:
				id, err := strconv.Atoi(tok.ivalue)
				if err != nil {
					return nil, errors.New("unknown rule type")
				}
				irule.Type = uint8(id)
			}
		case itemRuleID:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			irule.ID = uint8(id)
			irule.Ruleset = uint8(id) // Ceph: ruleset == rule id
		}
	}
	if !ended {
		return nil, errors.New("unexpected end of crush map (unterminated rule)")
	}
	if irule.Name == "" {
		return nil, errors.New("rule without a name")
	}
	return irule, nil
}

func (p *textParser) handleRuleStep() (*Step, error) {
	istep := &Step{}
	var chooseOp string

Loop:
	for {
		tok, done := p.l.lexNextToken()
		if done {
			break Loop
		}
		switch tok.itype {
		case itemEOF, itemRuleStepEnd:
			break Loop
		case itemComment:
			continue
		case itemRuleStepTake:
			istep.Op = "take"
			istep.Item = -1
		case itemRuleStepTakeType:
			istep.ItemName = tok.ivalue
		case itemRuleStepTakeClass:
			istep.ItemClass = tok.ivalue
		case itemRuleStepChoose:
			chooseOp = tok.ivalue // choose | chooseleaf
		case itemRuleStepChooseMSR:
			chooseOp = "choosemsr"
		case itemRuleStepChooseMode:
			switch chooseOp {
			case "choose":
				istep.Op = "choose_" + tok.ivalue
			case "chooseleaf":
				istep.Op = "chooseleaf_" + tok.ivalue
			}
		case itemRuleStepChooseNum:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			istep.Num = int32(id)
			if chooseOp == "choosemsr" {
				istep.Op = "choosemsr_firstn"
			}
		case itemRuleStepChooseType:
			istep.ItemType = tok.ivalue
		case itemRuleStepEmit:
			istep.Op = "emit"
		case itemRuleStepSetGenericName:
			istep.Op = tok.ivalue
		case itemRuleStepSetGeneric:
			id, err := strconv.Atoi(tok.ivalue)
			if err != nil {
				return nil, err
			}
			istep.Num = int32(id)
		}
	}
	return istep, nil
}
