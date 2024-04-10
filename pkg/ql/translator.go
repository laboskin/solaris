package ql

import (
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"strings"
)

type (
	// Translator struct allows to turn AST objects (Expression, Condition etc.) to
	// the statements according to the dialect provided
	Translator[T any] struct {
		dialect Dialect[T]
	}
)

// NewTranslator creates new Translator with dialects provided
func NewTranslator[T any](dialect Dialect[T]) Translator[T] {
	return Translator[T]{dialect: dialect}
}

// Translate translates the expression string to string according to the dialect of the translator
func (tr Translator[T]) Translate(sb *strings.Builder, expr string) error {
	expr = strings.TrimSpace(expr)
	if len(expr) == 0 {
		return nil
	}
	e, err := Parse(expr)
	if err != nil {
		return fmt.Errorf("failed to parse expression=%q: %w", expr, err)
	}
	if err = tr.Expression2Sql(sb, e); err != nil {
		return fmt.Errorf("failed to translate expression=%q: %w", expr, err)
	}
	return nil
}

// Expression2Sql turns the AST object e to the query string according to the dialect of the translator
func (tr Translator[T]) Expression2Sql(sb *strings.Builder, e *Expression) error {
	for i, oc := range e.Or {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		if err := tr.OrCondition2Sql(sb, oc); err != nil {
			return err
		}
	}
	return nil
}

// OrCondition2Sql turns the AST object oc to the query string according to the dialect of the translator
func (tr Translator[T]) OrCondition2Sql(sb *strings.Builder, oc *OrCondition) error {
	for i, xc := range oc.And {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		if err := tr.XCondition2Sql(sb, xc); err != nil {
			return err
		}
	}
	return nil
}

// XCondition2Sql turns the AST object xc to the query string according to the dialect of the translator
func (tr Translator[T]) XCondition2Sql(sb *strings.Builder, xc *XCondition) error {
	if xc.Not {
		sb.WriteString(" NOT ")
	}
	if xc.Expr != nil {
		sb.WriteString("(")
		defer sb.WriteString(")")
		return tr.Expression2Sql(sb, xc.Expr)
	}
	return tr.Condition2Sql(sb, xc.Cond)
}

// Condition2Sql turns the AST object c to the query string according to the dialect of the translator
func (tr Translator[T]) Condition2Sql(sb *strings.Builder, c *Condition) error {
	// param1
	p1 := c.FirstParam
	dp1, ok := tr.dialect[c.FirstParam.ID()]
	if !ok {
		return fmt.Errorf("unknown parameter %s: %w", p1.Name(false), errors.ErrInvalid)
	}
	if dp1.Flags&PfLValue == 0 {
		return fmt.Errorf("parameter %s cannot be on the left side of the condition: %w", p1.Name(false), errors.ErrInvalid)
	}
	if c.Op == "" {
		if dp1.Flags&PfNop == 0 {
			return fmt.Errorf("parameter %s should be compared with something in a condition: %w", p1.Name(false), errors.ErrInvalid)
		}
		return tr.Param2Sql(sb, &p1)
	}
	if dp1.Flags&PfNop != 0 {
		return fmt.Errorf("parameter %s cannot be compared (%s) in the condition: %w", p1.Name(false), c.Op, errors.ErrInvalid)
	}

	// param2
	p2 := c.SecondParam
	if p2 == nil {
		return fmt.Errorf("wrong condition for the param %s and the operation %q - no second parameter: %w", p1.Name(false), c.Op, errors.ErrInvalid)
	}
	dp2, ok := tr.dialect[p2.ID()]
	if !ok {
		return fmt.Errorf("unknown second parameter %s: %w", p2.Name(false), errors.ErrInvalid)
	}
	if dp2.Flags&PfRValue == 0 {
		return fmt.Errorf("parameter %s cannot be on the right side of the condition: %w", p2.Name(false), errors.ErrInvalid)
	}
	if dp2.Flags&PfNop != 0 {
		return fmt.Errorf("parameter %s cannot be compared (%s) in the condition: %w", p2.Name(false), c.Op, errors.ErrInvalid)
	}

	// operation
	op := strings.ToUpper(c.Op)
	switch op {
	case "<", ">", "<=", ">=", "!=", "=":
		if dp1.Flags&PfComparable == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the operation %s: %w", p1.Name(false), c.Op, errors.ErrInvalid)
		}
		if dp2.Flags&PfComparable == 0 {
			return fmt.Errorf("the second parameter %s is not applicable for the operation %s: %w", p2.Name(false), c.Op, errors.ErrInvalid)
		}
	case "IN":
		if dp1.Flags&PfInLike == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the IN : %w", p1.Name(false), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != ArrayParamID {
			return fmt.Errorf("the second parameter %s must be an array: %w", p2.Name(false), errors.ErrInvalid)
		}
	case "LIKE":
		if dp1.Flags&PfInLike == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the LIKE : %w", p1.Name(false), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != StringParamID {
			return fmt.Errorf("the right value(%s) of LIKE must be a string: %w", p2.Name(false), errors.ErrInvalid)
		}
	default:
		return fmt.Errorf("unknown operation %s: %w", c.Op, errors.ErrInvalid)
	}

	if err := tr.Param2Sql(sb, &p1); err != nil {
		return err
	}
	sb.WriteString(" ")
	sb.WriteString(op)
	sb.WriteString(" ")
	return tr.Param2Sql(sb, p2)
}

// Param2Sql turns the AST object p to the query string according to the dialect of the translator
func (tr Translator[T]) Param2Sql(sb *strings.Builder, p *Param) error {
	dp, ok := tr.dialect[p.ID()]
	if !ok {
		return fmt.Errorf("unknown parameter %s: %w", p.Name(false), errors.ErrInvalid)
	}
	if dp.TranslateF != nil {
		return dp.TranslateF(tr, sb, *p)
	}
	name := p.Name(true)
	sb.WriteString(name)
	if p.Function == nil {
		return nil
	}
	sb.WriteString("(")
	_ = tr.Params2Sql(sb, p.Function.Params)
	sb.WriteString(")")
	return nil
}

// Params2Sql turns the AST object pp to the query string according to the dialect of the translator
func (tr Translator[T]) Params2Sql(sb *strings.Builder, pp []*Param) error {
	for i, p := range pp {
		if i > 0 {
			sb.WriteString(", ")
		}
		if err := tr.Param2Sql(sb, p); err != nil {
			return err
		}
	}
	return nil
}
