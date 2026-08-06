//! FEEL AST — port of the sealed `FeelExpr` interface. Every node carries its `(start, end)`
//! character offsets within the source expression for determinism analysis and diagnostics
//! (offsets are 0-based, end-exclusive).

use crate::value::FeelValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Plus,
    Minus,
    Times,
    Div,
    /// Exponentiation (`**`) — binds tighter than `* /`, right operand may be a unary expression
    /// (`base ** -exp`).
    Pow,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FeelExpr {
    Literal {
        start: usize,
        end: usize,
        value: FeelValue,
    },
    /// Dotted path access, e.g. `payload.x.y.z` → segments `["payload", "x", "y", "z"]`.
    /// The parser guarantees at least one segment.
    Path {
        start: usize,
        end: usize,
        segments: Vec<String>,
    },
    Compare {
        start: usize,
        end: usize,
        left: Box<FeelExpr>,
        op: CompareOp,
        right: Box<FeelExpr>,
    },
    BoolOp {
        start: usize,
        end: usize,
        left: Box<FeelExpr>,
        op: LogicalOp,
        right: Box<FeelExpr>,
    },
    Not {
        start: usize,
        end: usize,
        arg: Box<FeelExpr>,
    },
    /// Unary arithmetic negation (`-x`) — a DISTINCT construct from binary subtraction (DMN-TCK
    /// 0099-arithmetic-negation), not desugared to `0 - x`: FEEL defines negation directly on a
    /// `number` (flip sign) or a `duration` (flip sign, either flavour), and as `null` for every
    /// other type (`0 - duration` is itself undefined — FEEL has no number-minus-duration
    /// arithmetic — so desugaring would wrongly reject `-@"P1D"` the same way).
    Negate {
        start: usize,
        end: usize,
        arg: Box<FeelExpr>,
    },
    Arith {
        start: usize,
        end: usize,
        left: Box<FeelExpr>,
        op: ArithOp,
        right: Box<FeelExpr>,
    },
    /// The parser guarantees a non-blank function name. `arg_names` runs parallel to `args`:
    /// `Some(name)` for a named argument `f(x: 1)`, `None` for a positional one. Builtins ignore
    /// the names (positional order); a user function binds by name when any are present.
    Call {
        start: usize,
        end: usize,
        name: String,
        args: Vec<FeelExpr>,
        arg_names: Vec<Option<String>>,
    },
    /// A FEEL function-definition literal `function(a, b) body` → a [`crate::value::FeelValue`]
    /// function. A parameter type annotation (`function(a: number) …`) is retained as the
    /// parameter's declared shape (one entry per parameter, [`crate::value::FeelTypeShape::Any`]
    /// for an unannotated one) — a non-conforming argument makes the call `null`, DMN §10.3.2.13's
    /// "never invoked" semantics for inline lambdas too (DMN-TCK 0082-feel-coercion#fd_002).
    FunctionDef {
        start: usize,
        end: usize,
        params: Vec<String>,
        param_shapes: Vec<crate::value::FeelTypeShape>,
        /// Grammar rule 55's optional `external` marker (`function(…) external {java: {…}}` /
        /// `{pmml: {…}}` — DMN 1.4 §10.3.2.13.3). When set, `body` is not FEEL logic to run at
        /// call time but the java/pmml binding context, classified at definition time into a
        /// [`crate::value::ExternalFunctionBinding`]; invoking the resulting function value is a
        /// deliberate semantic error (this engine does not execute external functions).
        external: bool,
        body: Box<FeelExpr>,
    },
    IfThenElse {
        start: usize,
        end: usize,
        cond: Box<FeelExpr>,
        then: Box<FeelExpr>,
        otherwise: Box<FeelExpr>,
    },
    /// FEEL list literal `[a, b, c]` → a [`crate::value::FeelValue::List`].
    ListLit {
        start: usize,
        end: usize,
        items: Vec<FeelExpr>,
    },
    /// FEEL inline context `{ key: expr, ... }` → a [`crate::value::FeelValue::Map`]. Entry order
    /// is preserved as parsed (the evaluator inserts into a `BTreeMap`, so lookup is by key).
    ContextLit {
        start: usize,
        end: usize,
        entries: Vec<(String, FeelExpr)>,
    },
    /// A FEEL range/interval `[a..b]`, `(a..b)`, `[a..b)`, `(a..b]` → a range value; open/closed
    /// bounds are carried by the inclusivity flags. Also the iterable source of `for`/`some`/`every`
    /// (integer ranges expand to their elements).
    ///
    /// `bracketed` distinguishes an actual interval-literal syntax (`[`/`(`/`]` delimited — always
    /// `true`) from the bracket-less `for i in a..b` iteration-domain form (`parse_iterable`,
    /// always `false`) — the two can produce a STRUCTURALLY IDENTICAL node (`for i in [2..1]
    /// return i` and `for i in 2..1 return i` both yield `from_inclusive: true, to_inclusive:
    /// true`), so this flag is the only way `sutra_evaluator::iterable_items` can tell a genuine
    /// descending interval VALUE (invalid to iterate — DMN-TCK 0084-feel-for-loops#decision_025)
    /// from the dedicated bidirectional for-loop domain syntax (valid either direction —
    /// #decision_007/008/009).
    Range {
        start: usize,
        end: usize,
        from: Box<FeelExpr>,
        to: Box<FeelExpr>,
        from_inclusive: bool,
        to_inclusive: bool,
        bracketed: bool,
    },
    /// FEEL quantified expression `some|every <var> in <source> satisfies <condition>` → a
    /// `Boolean` (three-valued disjunction/conjunction of the condition over the source list).
    Quantifier {
        start: usize,
        end: usize,
        every: bool,
        var: String,
        source: Box<FeelExpr>,
        condition: Box<FeelExpr>,
    },
    /// FEEL iteration `for <var> in <source>[, <var> in <source>]* return <body>` → a `List` of
    /// the body evaluated over the (cartesian) product of the sources.
    For {
        start: usize,
        end: usize,
        bindings: Vec<(String, FeelExpr)>,
        body: Box<FeelExpr>,
    },
    /// FEEL filter `source[predicate]` (§10.3.4.5): a numeric predicate indexes (1-based, negative
    /// from the end); any other predicate filters, evaluated per element with the element's own
    /// entries plus `item` in scope.
    Filter {
        start: usize,
        end: usize,
        source: Box<FeelExpr>,
        predicate: Box<FeelExpr>,
    },
    /// Field access on an arbitrary expression `base.field` — a `Map` yields the entry; a `List`
    /// projects the field over its elements (`people[…].name`).
    FieldAccess {
        start: usize,
        end: usize,
        base: Box<FeelExpr>,
        field: String,
    },
    /// FEEL type test `expr instance of <type>` → a boolean. `type_shape` is the fully parsed
    /// type expression — a (possibly multi-word) base type, or a structural `list<T>`/
    /// `context<k: T, ...>`/`range<T>` generic — as a [`crate::value::FeelTypeShape`] (the same
    /// shape machinery DMN §10.3.2.13 typeRef coercion uses).
    InstanceOf {
        start: usize,
        end: usize,
        expr: Box<FeelExpr>,
        type_shape: crate::value::FeelTypeShape,
    },
    /// FEEL membership `value in test` → a boolean. When `test` is a list the value must equal one
    /// element; otherwise it must equal the single test value.
    In {
        start: usize,
        end: usize,
        value: Box<FeelExpr>,
        test: Box<FeelExpr>,
    },
    /// Postfix invocation of an arbitrary expression `callee(args)` — not only a bare name (that
    /// case stays `Call`). Covers a parenthesised function literal called immediately
    /// (`(function(a) a)(10)`), a call chain that itself returns a function (`f()(4)`), and
    /// invoking a non-function value (`null()`, `123()`), which parses fine and is a runtime type
    /// error, not a syntax error (DMN 1.4 §10.3.2.11 / §10.3.4).
    Invoke {
        start: usize,
        end: usize,
        callee: Box<FeelExpr>,
        args: Vec<FeelExpr>,
        arg_names: Vec<Option<String>>,
    },
    /// A comparison-operator range value used as a first-class expression, not a unary test —
    /// `< e`, `<= e`, `> e`, `>= e`, `= e`, `!= e` (DMN 1.4 §10.3.2.11) — denoting a semi-infinite
    /// (or, for `=`/`!=`, degenerate single-point) range. Evaluates to a
    /// [`crate::value::FeelValue::Range`] tagged with the operator so it never structurally
    /// equals an ordinary interval literal sharing the same bound (`(< 10) != (null..10)` —
    /// DMN-TCK 0068-feel-equality `range_006`).
    OpenRange {
        start: usize,
        end: usize,
        op: CompareOp,
        bound: Box<FeelExpr>,
    },
}

impl FeelExpr {
    pub fn start(&self) -> usize {
        match self {
            FeelExpr::Literal { start, .. }
            | FeelExpr::Path { start, .. }
            | FeelExpr::Compare { start, .. }
            | FeelExpr::BoolOp { start, .. }
            | FeelExpr::Not { start, .. }
            | FeelExpr::Negate { start, .. }
            | FeelExpr::Arith { start, .. }
            | FeelExpr::Call { start, .. }
            | FeelExpr::ListLit { start, .. }
            | FeelExpr::ContextLit { start, .. }
            | FeelExpr::Range { start, .. }
            | FeelExpr::Quantifier { start, .. }
            | FeelExpr::For { start, .. }
            | FeelExpr::Filter { start, .. }
            | FeelExpr::FieldAccess { start, .. }
            | FeelExpr::InstanceOf { start, .. }
            | FeelExpr::FunctionDef { start, .. }
            | FeelExpr::In { start, .. }
            | FeelExpr::Invoke { start, .. }
            | FeelExpr::OpenRange { start, .. }
            | FeelExpr::IfThenElse { start, .. } => *start,
        }
    }

    pub fn end(&self) -> usize {
        match self {
            FeelExpr::Literal { end, .. }
            | FeelExpr::Path { end, .. }
            | FeelExpr::Compare { end, .. }
            | FeelExpr::BoolOp { end, .. }
            | FeelExpr::Not { end, .. }
            | FeelExpr::Negate { end, .. }
            | FeelExpr::Arith { end, .. }
            | FeelExpr::Call { end, .. }
            | FeelExpr::ListLit { end, .. }
            | FeelExpr::ContextLit { end, .. }
            | FeelExpr::Range { end, .. }
            | FeelExpr::Quantifier { end, .. }
            | FeelExpr::For { end, .. }
            | FeelExpr::Filter { end, .. }
            | FeelExpr::FieldAccess { end, .. }
            | FeelExpr::InstanceOf { end, .. }
            | FeelExpr::FunctionDef { end, .. }
            | FeelExpr::In { end, .. }
            | FeelExpr::Invoke { end, .. }
            | FeelExpr::OpenRange { end, .. }
            | FeelExpr::IfThenElse { end, .. } => *end,
        }
    }
}
