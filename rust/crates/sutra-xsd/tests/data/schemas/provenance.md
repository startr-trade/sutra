# Authored schema fixtures (sutra-xsd test data)

Every `.xsd` in this directory is **authored for this project**. None is a published schema,
and no published schema is reproduced here in whole or in part.

| File | Shape it exercises |
|---|---|
| `order.001.001.01.xsd` | The rich case: nested sequences, an unbounded repeated element, a choice-typed container, `simpleContent` + required attribute, a decimal restriction chain reachable only through that `simpleContent`, digits-restricting-`xs:string` ("numeric text"), enumerations, patterns, `minLength`/`maxLength`, `xs:length`, `xs:dateTime` / `xs:date` / `xs:boolean` / `xs:integer` |
| `invoice.002.001.01.xsd` | The same construct families arranged DIFFERENTLY: an inline root type, a choice directly under a named type, a *bounded* repeated element, decimal facets without `simpleContent`, a two-hop simple-type restriction chain, `xs:base64Binary` |

## Why authored rather than vendored

They are written in the **Standards-Editor idiom** a registered message definition uses — one
global `Document` root over a named envelope type, named simple types carrying the facets, no
`xs:import` / `xs:include` / `xs:group` — because that idiom is exactly what the Tier-1 subset
targets and what a module-codec author writes. Reproducing the idiom is what the suite needs;
reproducing anyone's published content is not, so this crate ships none.

The consequence is that this crate's tests are self-contained and carry no third-party
licensing surface at all: they compile, validate and shape-check bytes this project wrote.

## Changing them

These are ordinary fixtures — nothing is pinned to their byte offsets, so they may be edited
freely as long as `compile_subset` and `shape_tables` are updated with them.
`FIXTURES` in `tests/all/compile_subset.rs` pins the file COUNT, so adding or removing one is a
deliberate two-line act rather than a silent change in sweep breadth.
