# Authored schema fixture (sutra-schema-gen test data)

`test.002.001.03.xsd` is **authored for this project**. It is not a published schema and
reproduces no published message content.

It is written in the **Standards-Editor idiom** the generator parses — one global `Document`
root over a named message type, named simple types carrying the facets, `xs:any` for the
supplementary-data envelope — because that idiom is the generator's input contract. Its
namespace uses the registry URN shape (`urn:iso:std:iso:20022:tech:xsd:<message-type>`) with an
**invented** message code, because `emit::ISO_NS_PREFIX` derives the module name and the
`MESSAGE_TYPE` constant from that shape. The URN is the generator's input FORMAT; the content
under it is ours.

It is deliberately the smallest schema that still drives every emission path:

| Path | Construct |
|---|---|
| nested decode tables | `Document` → `OrderConfirmationV03` → `OrderLine1` |
| choice type (`is_choice`) | `OrderReference1Choice` |
| unbounded repeat (`repeated`) | `Line`, `SplmtryData` |
| optional element (`required: false`) | `SssnIdr`, `Dsclmr`, `DlvryDt`, `BckOrdrd` |
| `xs:any` (`has_any`) | `SupplementaryDataEnvelope1` |
| enumeration value table | `ConfirmationStatus1Code` |
| string facets | `Max35Text`, `Max350Text`, `Exact4AlphaNumericText` (pattern) |
| decimal facets | `ActiveCurrencyAndAmount_SimpleType`, `DecimalNumber` |
| `simpleContent` + required attribute | `ActiveCurrencyAndAmount` / `Ccy` |
| date / dateTime / boolean scalars | `ISODate`, `ISODateTime`, `YesNoIndicator` |

`tests/all/golden.rs` asserts that coverage structurally
(`the_golden_exercises_every_emission_path`), so a fixture edit that quietly drops a construct
fails rather than regenerating both sides together into a weaker gate.

Its committed emission lives next door at `../golden/test002v03.rs.golden`. That file is
generator OUTPUT, not hand-written: refresh it with

```
cargo run -p sutra-cli -- generate schema-handler <this crate>/tests/data/schemas <tmp>
```

and copy `test002v03.rs` over it, in the same commit as whatever generator change moved it.

Holding both here keeps the generator self-contained: it is a neutral tool that takes arbitrary
corpus and output paths, so its own gate must not depend on the location of any particular
corpus, nor on the generated crate a given distribution produces.
