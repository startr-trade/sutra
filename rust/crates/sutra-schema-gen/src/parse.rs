//! XSD → [`ParseResult`]: the codegen-relevant subset of the XSD reading rules fixed for
//! the ISO 20022 corpus. Named `complexType`s become
//! class models (document order); enumerated `simpleType`s become enum models; non-enum
//! restriction `simpleType`s become inline aliases to their base builtin (facets ride
//! along); `simpleContent`/`extension` yields a `value` field plus attributes; `xs:any`
//! is a wildcard.
//!
//! The corpus is flat (no inheritance, element ref, substitution groups, xs:all, mixed
//! content or anonymous types), so only that subset is implemented; anything richer would
//! surface as a byte diff against the committed tree and be caught by the `--check` gate.

use std::collections::HashMap;

use crate::model::{
    ClassModel, EnumModel, EnumValue, Facets, FieldModel, FieldType, ParseResult, Scalar,
};
use crate::xml::{self, Node, XSD_NS};

/// XSD builtin local name → the neutral scalar kind — the canonical mapping the generated
/// decoder/projection surface is fixed to. `None` for unknown names (they
/// resolve as same-schema named types) and for `anyType` (see [`scalar_or_opaque`]).
fn builtin_scalar(local: &str) -> Option<Scalar> {
    Some(match local {
        "string" | "anyURI" | "token" | "normalizedString" | "language" | "NMTOKEN"
        | "NMTOKENS" | "NCName" | "ID" | "IDREF" | "IDREFS" | "Name" => Scalar::Text,
        "boolean" => Scalar::Boolean,
        "int" | "long" | "short" | "byte" | "unsignedInt" | "unsignedShort" | "unsignedByte" => {
            Scalar::Int
        }
        "float" | "double" | "decimal" => Scalar::Decimal,
        "integer" | "unsignedLong" | "positiveInteger" | "nonNegativeInteger"
        | "negativeInteger" | "nonPositiveInteger" => Scalar::BigInt,
        "date" | "dateTime" | "time" | "gYear" | "gYearMonth" | "gMonth" | "gMonthDay" | "gDay" => {
            Scalar::DateTime
        }
        "duration" => Scalar::Duration,
        "QName" => Scalar::QName,
        "base64Binary" | "hexBinary" => Scalar::Bytes,
        _ => return None,
    })
}

/// Whether `local` is a known builtin name — a scalar or the opaque `anyType` (which is
/// accepted but not decoded). Alias chains resolve until they reach one of these.
fn is_builtin(local: &str) -> bool {
    local == "anyType" || builtin_scalar(local).is_some()
}

/// The [`FieldType`] for a known builtin name.
fn scalar_or_opaque(local: &str) -> FieldType {
    match builtin_scalar(local) {
        Some(scalar) => FieldType::Scalar(scalar),
        None => FieldType::Opaque, // anyType
    }
}

/// Per-parse alias state for non-enum simple-type typedefs.
struct Aliases {
    /// simpleType name → base XSD builtin local name.
    base: HashMap<String, String>,
    /// simpleType name → the facets accumulated along its restriction chain.
    facets: HashMap<String, Facets>,
}

/// Parse one XSD document into its class/enum model.
pub fn parse_xsd(bytes: &[u8]) -> Result<ParseResult, String> {
    let schema = xml::parse(bytes)?;
    if !schema.is_xsd("schema") {
        return Err("document root is not xs:schema".to_string());
    }

    let target_namespace = schema.attr("targetNamespace").map(str::to_string);

    let root_elements = collect_root_elements(&schema);
    let aliases = collect_simple_type_aliases(&schema);

    let mut result = ParseResult {
        target_namespace,
        classes: Vec::new(),
        enums: Vec::new(),
    };

    for ct in schema.xsd_children("complexType") {
        if let Some(cm) = parse_complex_type(ct, &root_elements, &aliases) {
            result.classes.push(cm);
        }
    }
    for st in schema.xsd_children("simpleType") {
        if let Some(em) = parse_simple_type(st) {
            result.enums.push(em);
        }
    }

    Ok(result)
}

/// type name → root element name, for top-level `<xs:element name="X" type="Y"/>`.
fn collect_root_elements(schema: &Node) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for el in schema.xsd_children("element") {
        let name = el.attr("name");
        let ty = el.attr("type").map(local_name);
        if let (Some(name), Some(ty)) = (name, ty) {
            map.entry(ty.to_string())
                .or_insert_with(|| name.to_string());
        }
    }
    map
}

fn parse_complex_type(
    ct: &Node,
    root_elements: &HashMap<String, String>,
    aliases: &Aliases,
) -> Option<ClassModel> {
    let type_name = ct.attr("name")?;
    if type_name.is_empty() {
        return None;
    }

    let mut cm = ClassModel {
        name: crate::names::to_class_name(type_name),
        xml_type_name: type_name.to_string(),
        root_element_name: root_elements.get(type_name).cloned(),
        javadoc: extract_documentation(ct),
        fields: Vec::new(),
    };

    if let Some(simple_content) = ct.first_xsd_child("simpleContent") {
        if let Some(extension) = simple_content.first_xsd_child("extension") {
            if let Some(base) = extension.attr("base") {
                let mut value = FieldModel {
                    xml_name: "value".to_string(),
                    is_xml_value: true,
                    required: true,
                    ..FieldModel::default()
                };
                resolve_field_type(&mut value, Some(base), aliases);
                cm.fields.push(value);
            }
            collect_attribute_fields(extension, &mut cm, aliases);
        }
    } else if let Some(complex_content) = ct.first_xsd_child("complexContent") {
        // Not exercised by the ISO corpus; kept so a future schema surfaces as a diff.
        if let Some(extension) = complex_content.first_xsd_child("extension") {
            collect_fields(extension, &mut cm, aliases);
        }
    } else {
        collect_fields(ct, &mut cm, aliases);
    }

    Some(cm)
}

fn collect_fields(container: &Node, cm: &mut ClassModel, aliases: &Aliases) {
    for group_tag in ["sequence", "all", "choice"] {
        if let Some(group) = container.first_xsd_child(group_tag) {
            collect_element_fields(group, cm, aliases);
        }
    }
    collect_attribute_fields(container, cm, aliases);
}

fn collect_element_fields(group: &Node, cm: &mut ClassModel, aliases: &Aliases) {
    for child in &group.children {
        if child.ns != XSD_NS {
            continue;
        }
        match child.local.as_str() {
            "element" => {
                if let Some(field) = parse_element_field(child, aliases) {
                    cm.fields.push(field);
                }
            }
            "any" => cm.fields.push(parse_any_field(child)),
            "sequence" | "all" | "choice" => collect_element_fields(child, cm, aliases),
            _ => {}
        }
    }
}

fn collect_attribute_fields(container: &Node, cm: &mut ClassModel, aliases: &Aliases) {
    for child in container.xsd_children("attribute") {
        if let Some(field) = parse_attribute_field(child, aliases) {
            cm.fields.push(field);
        }
    }
}

fn parse_element_field(el: &Node, aliases: &Aliases) -> Option<FieldModel> {
    let xml_name = el.attr("name")?; // corpus has no `ref` elements
    let (required, is_list) = occurs(el);

    let mut fm = FieldModel {
        xml_name: xml_name.to_string(),
        required,
        is_list,
        ..FieldModel::default()
    };
    resolve_field_type(&mut fm, el.attr("type"), aliases);
    Some(fm)
}

fn parse_any_field(any: &Node) -> FieldModel {
    let (required, is_list) = occurs(any);
    FieldModel {
        xml_name: "any".to_string(),
        field_type: FieldType::Opaque,
        required,
        is_list,
        is_any_element: true,
        ..FieldModel::default()
    }
}

fn parse_attribute_field(attr_el: &Node, aliases: &Aliases) -> Option<FieldModel> {
    let xml_name = attr_el.attr("name")?;
    let required = attr_el
        .attr("use")
        .is_some_and(|u| u.eq_ignore_ascii_case("required"));
    let mut fm = FieldModel {
        xml_name: xml_name.to_string(),
        required,
        is_attribute: true,
        ..FieldModel::default()
    };
    resolve_field_type(&mut fm, attr_el.attr("type"), aliases);
    Some(fm)
}

/// (required, is_list) from minOccurs/maxOccurs.
fn occurs(el: &Node) -> (bool, bool) {
    let min = el.attr("minOccurs");
    let max = el.attr("maxOccurs");
    let required = match min {
        None => true,
        Some(m) => m.parse::<i64>().map(|n| n >= 1).unwrap_or(true),
    };
    let is_list = match max {
        Some("unbounded") => true,
        Some(m) => m.parse::<i64>().map(|n| n > 1).unwrap_or(false),
        None => false,
    };
    (required, is_list)
}

fn resolve_field_type(fm: &mut FieldModel, type_attr: Option<&str>, aliases: &Aliases) {
    let type_attr = match type_attr {
        Some(t) if !t.is_empty() => t,
        _ => {
            fm.field_type = FieldType::Opaque;
            return;
        }
    };
    let local = local_name(type_attr);

    // Typedef alias (highest priority): resolves to its base builtin; the accumulated
    // facets ride along to the field.
    if let Some(base) = aliases.base.get(local) {
        if is_builtin(base) {
            fm.field_type = scalar_or_opaque(base);
            if let Some(facets) = aliases.facets.get(local) {
                if !facets.is_empty() {
                    fm.facets = facets.clone();
                }
            }
            return;
        }
    }

    if is_builtin(local) {
        fm.field_type = scalar_or_opaque(local);
        return;
    }

    // Same-schema named type (complexType or enumerated simpleType).
    fm.field_type = FieldType::Named(crate::names::to_class_name(local));
}

fn parse_simple_type(st: &Node) -> Option<EnumModel> {
    let type_name = st.attr("name")?;
    if type_name.is_empty() {
        return None;
    }
    let restriction = descendant(st, "restriction")?;
    let enumerations: Vec<&Node> = restriction.xsd_children("enumeration").collect();
    if enumerations.is_empty() {
        return None;
    }

    let mut em = EnumModel {
        name: crate::names::to_class_name(type_name),
        values: Vec::new(),
    };
    for enum_el in enumerations {
        if let Some(value) = enum_el.attr("value") {
            em.values.push(EnumValue {
                canonical_name: crate::names::to_enum_constant(value),
                xml_value: value.to_string(),
            });
        }
    }
    Some(em)
}

// ---------------------------------------------------------------------------
// simpleType typedef aliases (non-enum restrictions → base builtin + facets)
// ---------------------------------------------------------------------------

fn collect_simple_type_aliases(schema: &Node) -> Aliases {
    let mut raw_base: Vec<(String, String)> = Vec::new();
    let mut raw_facets: HashMap<String, Facets> = HashMap::new();

    for st in schema.xsd_children("simpleType") {
        let name = match st.attr("name") {
            Some(n) if !n.is_empty() => n,
            _ => continue,
        };
        let restriction = match descendant(st, "restriction") {
            Some(r) => r,
            None => continue,
        };
        // Enum-bearing restrictions become EnumModel, not aliases.
        if restriction.xsd_children("enumeration").next().is_some() {
            continue;
        }
        let base = match restriction.attr("base") {
            Some(b) if !b.is_empty() => b,
            _ => continue,
        };
        raw_base.push((name.to_string(), local_name(base).to_string()));
        raw_facets.insert(name.to_string(), extract_facets(restriction));
    }

    let base_lookup: HashMap<&str, &str> = raw_base
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let mut aliases = Aliases {
        base: HashMap::new(),
        facets: HashMap::new(),
    };
    for (name, first_base) in &raw_base {
        let mut resolved: &str = first_base;
        let mut accumulated = raw_facets.get(name).cloned().unwrap_or_default();
        let mut hops = 0;
        while !is_builtin(resolved) && base_lookup.contains_key(resolved) && hops < 16 {
            if let Some(next_facets) = raw_facets.get(resolved) {
                merge_facets(&mut accumulated, next_facets);
            }
            resolved = base_lookup[resolved];
            hops += 1;
        }
        if is_builtin(resolved) {
            aliases.base.insert(name.clone(), resolved.to_string());
            if !accumulated.is_empty() {
                aliases.facets.insert(name.clone(), accumulated);
            }
        }
    }
    aliases
}

fn extract_facets(restriction: &Node) -> Facets {
    let mut facets = Facets::default();
    for f in &restriction.children {
        if f.ns != XSD_NS {
            continue;
        }
        let value = match f.attr("value") {
            Some(v) => v,
            None => continue,
        };
        match f.local.as_str() {
            "minLength" => facets.min_length = value.parse().ok(),
            "maxLength" => facets.max_length = value.parse().ok(),
            "length" => facets.length = value.parse().ok(),
            "pattern" => facets.patterns.push(value.to_string()),
            "minInclusive" => facets.min_inclusive = Some(value.to_string()),
            "maxInclusive" => facets.max_inclusive = Some(value.to_string()),
            "minExclusive" => facets.min_exclusive = Some(value.to_string()),
            "maxExclusive" => facets.max_exclusive = Some(value.to_string()),
            "totalDigits" => facets.total_digits = value.parse().ok(),
            "fractionDigits" => facets.fraction_digits = value.parse().ok(),
            _ => {}
        }
    }
    facets
}

/// Merge `src` into `dst`, filling only unset fields; patterns union (order-preserving).
fn merge_facets(dst: &mut Facets, src: &Facets) {
    if dst.min_length.is_none() {
        dst.min_length = src.min_length;
    }
    if dst.max_length.is_none() {
        dst.max_length = src.max_length;
    }
    if dst.length.is_none() {
        dst.length = src.length;
    }
    if dst.min_inclusive.is_none() {
        dst.min_inclusive = src.min_inclusive.clone();
    }
    if dst.max_inclusive.is_none() {
        dst.max_inclusive = src.max_inclusive.clone();
    }
    if dst.min_exclusive.is_none() {
        dst.min_exclusive = src.min_exclusive.clone();
    }
    if dst.max_exclusive.is_none() {
        dst.max_exclusive = src.max_exclusive.clone();
    }
    if dst.total_digits.is_none() {
        dst.total_digits = src.total_digits;
    }
    if dst.fraction_digits.is_none() {
        dst.fraction_digits = src.fraction_digits;
    }
    for p in &src.patterns {
        if !dst.patterns.contains(p) {
            dst.patterns.push(p.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn extract_documentation(el: &Node) -> Option<String> {
    let annotation = el.first_xsd_child("annotation")?;
    let documentation = annotation.first_xsd_child("documentation")?;
    let text = documentation.text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// First XSD-namespace descendant (preorder) with the given local name.
fn descendant<'n>(node: &'n Node, local: &str) -> Option<&'n Node> {
    for child in &node.children {
        if child.ns == XSD_NS && child.local == local {
            return Some(child);
        }
        if let Some(found) = descendant(child, local) {
            return Some(found);
        }
    }
    None
}

/// Strips a namespace prefix: `xs:string` → `string`.
fn local_name(qname: &str) -> &str {
    match qname.find(':') {
        Some(i) => &qname[i + 1..],
        None => qname,
    }
}
