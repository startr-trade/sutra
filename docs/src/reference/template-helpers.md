# Template helpers

A Handlebars template (`templates/*.hbs`) renders in strict mode: a value that resolves to nothing
(a missing key, a misspelt path, a null part-way along a path) fails the render. `{{x}}` renders
`x` verbatim, with no escaping, because templates produce XML, JSON and plain-text messages, not
HTML.

The engine registers the helpers below and no others. Helper arguments are the exception to strict
mode: a missing path is a null argument, never a failed render. So `(eq maybe "X")` can test for
absence, and `{{coalesce maybe "default"}}` can supply a default.

| Helper | Renders |
|---|---|
| `eq a b`, `neq a b` | `true` or `false`. Numbers compare as numbers, so `1` equals `1.0`. |
| `gt a b`, `gte a b`, `lt a b`, `lte a b` | `true` or `false`. Two numbers compare as numbers and two strings in code-point order; anything else is `false`. |
| `and a b …`, `or a b …`, `not a` | `true` or `false`, with Handlebars truthiness: `false`, null, `""`, `0` and `[]` are false. |
| `{{#let value as \|name\|}}…{{/let}}` | Its body, always, with `value` bound to `name`. A null binding fails the render only where the body uses it. |
| `substring value from to` | The characters of `value` from index `from` up to, not including, `to` (to the end if `to` is left out). A range outside the value fails the render. |
| `replace value "a" "b"` | `value` with every `a` replaced by `b`, literally (no patterns). |
| `coalesce value fallback …` | The first argument that is not null. |
| `xmlEscape value` | `value` with `&` `<` `>` `"` `'` replaced by `&amp;` `&lt;` `&gt;` `&quot;` `&apos;`. A null or missing value renders empty. (0.6.25) |

`uuid` and `now` are not helpers: the engine puts them in the render model, so a template reads
them like any other value.

## `xmlEscape` {#xmlescape}

Use `xmlEscape` for any free text that goes into an XML message: a name, a note, an address
line. Without it, a customer called `O'Brien & Sons` or a note containing `<` produces a
message that is not well-formed XML, and the receiver refuses it.

```handlebars
<Receipt customer="{{xmlEscape order.customer}}"><Note>{{xmlEscape order.note}}</Note></Receipt>
```

- It is safe in element content and in attribute values in either kind of quotes.
- Every other character passes through unchanged, non-Latin scripts included.
- A number or boolean renders in its usual text form. A null or missing value renders empty,
  where `{{order.note}}` on a missing value would fail the render.
- Escaping is opt-in per value. `{{x}}` stays verbatim, so existing templates render as before.
  Don't put `xmlEscape` on a value that already holds XML markup you want kept: it escapes that
  too.

[Escaping free text in an XML template](../building/escaping-xml-in-templates.md) runs it end to
end.
