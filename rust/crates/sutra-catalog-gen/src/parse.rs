//! `syn`-based extraction of one source file into the [`Parsed`] shape.
//!
//! References are read from **path-qualified `use` trees** only — never by scanning prose for
//! capitalised words (the name-collision defect the earlier catalog once hit). Name resolution of
//! those paths to concrete pages happens later, in [`crate::resolve`].

use syn::{ImplItem, Item, UseTree};

use crate::model::{Item as ModelItem, ItemKind, Method, ModDecl, TraitImpl, UseLeaf};
use crate::util::{doc_first_paragraph, tidy_tokens, vis_str};

/// Everything the renderer/resolver needs from a single `*.rs` file.
#[derive(Default)]
pub struct Parsed {
    pub module_doc: Option<String>,
    pub items: Vec<ModelItem>,
    pub methods: Vec<Method>,
    pub trait_impls: Vec<TraitImpl>,
    pub uses: Vec<UseLeaf>,
    pub child_mods: Vec<ModDecl>,
}

/// Parse `src` (a whole file's text). A syntactically-invalid file yields an empty [`Parsed`]
/// rather than aborting the whole run — a best-effort catalog is better than none.
pub fn parse_file(src: &str) -> Parsed {
    let file = match syn::parse_file(src) {
        Ok(f) => f,
        Err(_) => return Parsed::default(),
    };
    let mut out = Parsed {
        module_doc: doc_first_paragraph(&file.attrs),
        ..Parsed::default()
    };
    collect(&file.items, "", &[], &mut out);
    out.child_mods = collect_mod_decls(&file.items);

    out.items.sort_by_key(|i| (i.kind, i.name.to_lowercase()));
    out.methods.sort_by(|a, b| {
        (a.impl_ty.to_lowercase(), a.name.to_lowercase())
            .cmp(&(b.impl_ty.to_lowercase(), b.name.to_lowercase()))
    });
    out.trait_impls.sort_by(|a, b| {
        (a.type_name.to_lowercase(), a.trait_name.to_lowercase())
            .cmp(&(b.type_name.to_lowercase(), b.trait_name.to_lowercase()))
    });
    out
}

/// Walk items, flattening inline-module members with a `mod::` display prefix. `mod_stack` is
/// the inline-module path of the items being walked (for [`UseLeaf::in_module`]).
fn collect(items: &[Item], prefix: &str, mod_stack: &[String], out: &mut Parsed) {
    for item in items {
        match item {
            Item::Use(u) => {
                let mut leaves = Vec::new();
                flatten_use(&u.tree, &mut Vec::new(), &mut leaves);
                out.uses.extend(leaves.into_iter().map(|path| UseLeaf {
                    in_module: mod_stack.to_vec(),
                    path,
                }));
            }
            Item::Const(c) => out.items.push(ModelItem {
                kind: ItemKind::Constant,
                name: format!("{prefix}{}", c.ident),
                vis: vis_str(&c.vis),
                signature: None,
                doc: doc_first_paragraph(&c.attrs),
            }),
            Item::Static(s) => out.items.push(ModelItem {
                kind: ItemKind::Static,
                name: format!("{prefix}{}", s.ident),
                vis: vis_str(&s.vis),
                signature: None,
                doc: doc_first_paragraph(&s.attrs),
            }),
            Item::Struct(s) => out.items.push(ModelItem {
                kind: ItemKind::Struct,
                name: format!("{prefix}{}", s.ident),
                vis: vis_str(&s.vis),
                signature: None,
                doc: doc_first_paragraph(&s.attrs),
            }),
            Item::Enum(e) => out.items.push(ModelItem {
                kind: ItemKind::Enum,
                name: format!("{prefix}{}", e.ident),
                vis: vis_str(&e.vis),
                signature: None,
                doc: doc_first_paragraph(&e.attrs),
            }),
            Item::Union(u) => out.items.push(ModelItem {
                kind: ItemKind::Union,
                name: format!("{prefix}{}", u.ident),
                vis: vis_str(&u.vis),
                signature: None,
                doc: doc_first_paragraph(&u.attrs),
            }),
            Item::Trait(t) => out.items.push(ModelItem {
                kind: ItemKind::Trait,
                name: format!("{prefix}{}", t.ident),
                vis: vis_str(&t.vis),
                signature: None,
                doc: doc_first_paragraph(&t.attrs),
            }),
            Item::TraitAlias(t) => out.items.push(ModelItem {
                kind: ItemKind::Trait,
                name: format!("{prefix}{}", t.ident),
                vis: vis_str(&t.vis),
                signature: None,
                doc: doc_first_paragraph(&t.attrs),
            }),
            Item::Type(t) => out.items.push(ModelItem {
                kind: ItemKind::TypeAlias,
                name: format!("{prefix}{}", t.ident),
                vis: vis_str(&t.vis),
                signature: None,
                doc: doc_first_paragraph(&t.attrs),
            }),
            Item::Fn(f) => out.items.push(ModelItem {
                kind: ItemKind::Function,
                name: format!("{prefix}{}", f.sig.ident),
                vis: vis_str(&f.vis),
                signature: Some(tidy_tokens(&f.sig)),
                doc: doc_first_paragraph(&f.attrs),
            }),
            Item::Macro(m) => {
                if let Some(ident) = &m.ident {
                    out.items.push(ModelItem {
                        kind: ItemKind::Macro,
                        name: format!("{prefix}{ident}"),
                        // `macro_rules!` visibility is `#[macro_export]`, not a `pub` token.
                        vis: if has_macro_export(&m.attrs) {
                            "pub".to_string()
                        } else {
                            String::new()
                        },
                        signature: None,
                        doc: doc_first_paragraph(&m.attrs),
                    });
                }
            }
            Item::Mod(m) => {
                // `#[cfg(test)]` modules are test scaffolding, not catalogued surface — the
                // catalog documents the shipped module; their items and `use`s are skipped.
                if is_cfg_test(&m.attrs) {
                    continue;
                }
                out.items.push(ModelItem {
                    kind: ItemKind::Module,
                    name: format!("{prefix}{}", m.ident),
                    vis: vis_str(&m.vis),
                    signature: None,
                    doc: doc_first_paragraph(&m.attrs),
                });
                if let Some((_, inner)) = &m.content {
                    let nested = format!("{prefix}{}::", m.ident);
                    let mut stack = mod_stack.to_vec();
                    stack.push(m.ident.to_string());
                    collect(inner, &nested, &stack, out);
                }
            }
            Item::Impl(im) => collect_impl(im, out),
            _ => {}
        }
    }
}

/// Harvest methods + the `impl Trait for Type` relationship from one `impl` block.
fn collect_impl(im: &syn::ItemImpl, out: &mut Parsed) {
    let ty = tidy_tokens(&im.self_ty);
    let type_name = short_type(&ty);
    if let Some((_, path, _)) = &im.trait_ {
        out.trait_impls.push(TraitImpl {
            trait_name: short_type(&tidy_tokens(path)),
            type_name: type_name.clone(),
        });
    }
    for it in &im.items {
        if let ImplItem::Fn(f) = it {
            out.methods.push(Method {
                impl_ty: type_name.clone(),
                name: f.sig.ident.to_string(),
                vis: vis_str(&f.vis),
                signature: tidy_tokens(&f.sig),
                doc: doc_first_paragraph(&f.attrs),
            });
        }
    }
}

fn has_macro_export(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| a.path().is_ident("macro_export"))
}

/// True for `#[cfg(test)]` (exactly — compound predicates like `cfg(all(test, …))` stay in).
fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg")
            && matches!(&a.meta, syn::Meta::List(l) if l.tokens.to_string().trim() == "test")
    })
}

/// Drop generic arguments and leading path segments from a rendered type so relationships and
/// method groupings key on the bare type name (`Vec<Foo>` → `Vec`, `a::b::Ty` → `Ty`).
fn short_type(ty: &str) -> String {
    let head = ty.split(['<', ' ']).next().unwrap_or(ty);
    head.rsplit("::").next().unwrap_or(head).to_string()
}

/// Flatten a `use` tree into one `Vec<String>` per imported leaf (rename ignored, glob → `*`).
fn flatten_use(tree: &UseTree, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    match tree {
        UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            flatten_use(&p.tree, prefix, out);
            prefix.pop();
        }
        UseTree::Name(n) => {
            let mut leaf = prefix.clone();
            leaf.push(n.ident.to_string());
            out.push(leaf);
        }
        UseTree::Rename(r) => {
            let mut leaf = prefix.clone();
            leaf.push(r.ident.to_string());
            out.push(leaf);
        }
        UseTree::Glob(_) => {
            let mut leaf = prefix.clone();
            leaf.push("*".to_string());
            out.push(leaf);
        }
        UseTree::Group(g) => {
            for t in &g.items {
                flatten_use(t, prefix, out);
            }
        }
    }
}

/// Top-level `mod` declarations (with nested children for inline modules), for module-tree
/// building in [`crate::workspace`].
fn collect_mod_decls(items: &[Item]) -> Vec<ModDecl> {
    let mut out = Vec::new();
    for item in items {
        if let Item::Mod(m) = item {
            if is_cfg_test(&m.attrs) {
                continue;
            }
            let (inline, children) = match &m.content {
                Some((_, inner)) => (true, collect_mod_decls(inner)),
                None => (false, Vec::new()),
            };
            out.push(ModDecl {
                name: m.ident.to_string(),
                inline,
                children,
            });
        }
    }
    out
}
