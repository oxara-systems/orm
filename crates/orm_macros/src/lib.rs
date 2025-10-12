use std::collections::{BTreeSet, HashMap, HashSet};

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use regex::Regex;
use syn::{
    parse_macro_input, parse_quote, spanned::Spanned, Data, DeriveInput, Error, Expr, Field,
    Fields, FnArg, Ident, ItemFn, Lit, ReturnType, Stmt,
};

struct Table {
    fields: Vec<Field>,
    fns: Vec<proc_macro2::TokenStream>,
    placeholder_re: Regex,
    primary_keys: Vec<Field>,
    primary_keys_set: HashSet<Field>,
    struct_name: Ident,
    table_name: String,
}

impl Table {
    fn column_names_typed_sql(&self) -> String {
        self.fields
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().unwrap();
                // let Type::Path(ty) = &field.ty else {
                //     panic!("expected type to be a path");
                // };
                format!(
                    r#""{name}" as "{name}: _""#,
                    // ty.path.to_token_stream().to_string()
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn delete(&mut self) {
        let table_name = &self.table_name;
        let where_clause = self.where_primary_keys_sql(&mut 0);
        let self_bindings: Vec<_> = self
            .primary_keys
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().unwrap();
                quote! { self.#name as _ }
            })
            .collect();
        let query = &format!(r#"DELETE FROM "{table_name}" WHERE {where_clause}"#);
        self.fns.push(quote! {
            pub async fn delete<'a>(self, tx: &mut impl orm::WritableTransaction) -> orm::sqlx::Result<orm::sqlx::postgres::PgQueryResult> {
                orm::sqlx::query!(#query, #(#self_bindings),*).execute(tx.connection()).await
            }
        });
    }

    fn get(&mut self) {
        let struct_name = &self.struct_name;
        let table_name = &self.table_name;
        let column_names_typed = self.column_names_typed_sql();
        let params: Vec<FnArg> = self
            .primary_keys
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().unwrap();
                let ty = &field.ty;
                parse_quote! { #name: &#ty }
            })
            .collect();
        let where_clause = self.where_primary_keys_sql(&mut 0);
        let bindings: Vec<_> = self
            .primary_keys
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().unwrap();
                quote! { #name as _ }
            })
            .collect();
        let sql = &format!(
            r#"SELECT {column_names_typed} FROM "{table_name}" WHERE {where_clause} LIMIT 1"#
        );
        self.fns.push(quote! {
            pub async fn get(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<Self>> {
                orm::sqlx::query_as!(#struct_name, #sql, #(#bindings),*)
                    .fetch_optional(tx.connection()).await
            }
        });
        if self.primary_keys.len() <= 1 {
            return;
        }
        for field in &self.primary_keys {
            let name = field.ident.as_ref().unwrap();
            let ty = &field.ty;
            let sql =
                &format!(r#"SELECT {column_names_typed} FROM "{table_name}" WHERE "{name}" = $1"#);
            let fn_name = Ident::new(&format!("list_by_{name}"), Span::call_site());
            self.fns.push(quote! {
                pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #name: &#ty) -> orm::sqlx::Result<Vec<Self>> {
                    orm::sqlx::query_as!(#struct_name, #sql, #name as _)
                        .fetch_all(tx.connection()).await
                }
            });
        }
    }

    fn insert(&mut self) {
        let struct_name = &self.struct_name;
        let table_name = &self.table_name;

        let mut bindings = Vec::new();
        let mut column_names = Vec::new();
        let mut parameters = Vec::new();
        let mut return_column_names = Vec::new();
        for (i, field) in self.fields.iter().enumerate() {
            let name = field.ident.as_ref().unwrap();
            bindings.push(quote! { self.#name as _ });
            column_names.push(format!(r#""{name}""#));
            parameters.push(format!(r#"${}"#, i + 1));
            return_column_names.push(format!(r#""{name}" as "{name}: _""#));
        }
        let column_names = column_names.join(", ");
        let parameters = parameters.join(", ");
        let return_column_names = return_column_names.join(", ");

        let query = &format!(
            r#"INSERT INTO "{table_name}" ({column_names}) VALUES ({parameters}) RETURNING {return_column_names}"#,
        );
        self.fns.push(quote! {
            pub async fn insert(self, tx: &mut impl orm::WritableTransaction) -> orm::sqlx::Result<Self> {
                orm::sqlx::query_as!(#struct_name, #query, #(#bindings),*)
                    .fetch_one(tx.connection()).await
            }
        });
    }

    fn update(&mut self) {
        let struct_name = &self.struct_name;
        let table_name = &self.table_name;
        let return_column_names = self.column_names_typed_sql();

        let mut bindings = Vec::new();
        let mut binding_index = 0;

        let mut updates = Vec::new();
        for field in &self.fields {
            if self.primary_keys_set.contains(field) {
                continue;
            }
            let name = field.ident.as_ref().unwrap();
            binding_index += 1;
            updates.push(format!(r#""{name}" = ${binding_index}"#));
            bindings.push(quote! { self.#name as _ });
        }
        if updates.is_empty() {
            return;
        }
        let updates = updates.join(", ");

        let where_clause = self.where_primary_keys_sql(&mut binding_index);
        for field in &self.primary_keys {
            let name = field.ident.as_ref().unwrap();
            bindings.push(quote! { self.#name as _ });
        }

        let query = &format!(
            r#"UPDATE "{table_name}" SET {updates} WHERE {where_clause} RETURNING {return_column_names}"#,
        );
        self.fns.push(quote! {
            pub async fn update(self, tx: &mut impl orm::WritableTransaction) -> orm::sqlx::Result<Self> {
                orm::sqlx::query_as!(#struct_name, #query, #(#bindings),*).fetch_one(tx.connection()).await
            }
        });
    }

    fn upsert(&mut self) {
        let struct_name = &self.struct_name;
        let table_name = &self.table_name;

        let mut binding_index = 0;
        let mut bindings = Vec::new();
        let mut column_names = Vec::new();
        let mut parameters = Vec::new();
        let mut primary_keys = Vec::new();
        let mut return_column_names = Vec::new();
        let mut updates = Vec::new();
        for field in &self.fields {
            let name = field.ident.as_ref().unwrap();
            binding_index += 1;
            bindings.push(quote! { self.#name as _ });
            column_names.push(format!(r#""{name}""#));
            parameters.push(format!(r#"${binding_index}"#));
            return_column_names.push(format!(r#""{name}" as "{name}: _""#));
            if self.primary_keys_set.contains(field) {
                primary_keys.push(format!(r#""{name}""#));
            } else {
                updates.push(format!(r#""{name}" = ${binding_index}"#));
            }
        }
        if updates.is_empty() {
            return;
        }
        let column_names = column_names.join(", ");
        let parameters = parameters.join(", ");
        let primary_keys = primary_keys.join(", ");
        let return_column_names = return_column_names.join(", ");
        let updates = updates.join(", ");

        let query = &format!(
            r#"INSERT INTO "{table_name}" ({column_names}) VALUES ({parameters}) ON CONFLICT({primary_keys}) DO UPDATE SET {updates} RETURNING {return_column_names}"#,
        );
        self.fns.push(quote! {
            pub async fn upsert(self, tx: &mut impl orm::WritableTransaction) -> orm::sqlx::Result<Self> {
                orm::sqlx::query_as!(#struct_name, #query, #(#bindings),*)
                    .fetch_one(tx.connection()).await
            }
        });
    }

    fn userfn(&mut self, item_fn: ItemFn) {
        macro_rules! fail {
            ($span:expr, $message:expr) => {
                self.fns
                    .push(Error::new($span, $message).to_compile_error());
                return;
            };
        }

        let fn_name = item_fn.sig.ident;
        let fn_name_string = fn_name.to_string();

        let ret = item_fn.sig.output;

        // query
        let Some(query) = item_fn.block.stmts.first() else {
            fail!(item_fn.block.span(), "expected query string literal");
        };
        let Stmt::Expr(query, _) = query else {
            fail!(query.span(), "expected query string literal");
        };
        let Expr::Lit(query) = query else {
            fail!(query.span(), "expected query string literal");
        };
        let Lit::Str(query) = &query.lit else {
            fail!(query.span(), "expected query string literal");
        };
        let query = query.value();

        let struct_name = &self.struct_name;
        let table_name = &self.table_name;
        let column_names_typed = self.column_names_typed_sql();

        let mut params: Vec<FnArg> = Vec::new();
        let mut bindings = Vec::new();
        for arg in &item_fn.sig.inputs {
            let FnArg::Typed(arg) = &arg else {
                fail!(arg.span(), "expected non-self parameter");
            };
            let name = &arg.pat;
            let ty = &arg.ty;
            params.push(parse_quote! { #name: #ty });
            bindings.push(quote! { #name as _ });
        }

        let placeholders: BTreeSet<_> = self
            .placeholder_re
            .find_iter(&query)
            .map(|x| x.as_str().to_string())
            .collect();
        if placeholders.len() != params.len() {
            fail!(
                item_fn.sig.inputs.span(),
                format!(
                    "{} expects {} parameters, but has {}",
                    fn_name,
                    placeholders.len(),
                    params.len()
                )
            );
        }

        if fn_name_string.starts_with("count_") {
            let sql = &format!(r#"SELECT COUNT(*) FROM "{table_name}" {query}"#,);
            self.fns.push(quote! {
                pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<i64> {
                    let count = orm::sqlx::query_scalar!(#sql, #(#bindings),*)
                        .fetch_one(tx.connection()).await?;
                    Ok(count.unwrap_or_default())
                }
            });
        } else if fn_name_string.starts_with("exist_") || fn_name_string.starts_with("is_") {
            let sql =
                &format!(r#"SELECT COUNT(*) > 0 AS "bool!" FROM "{table_name}" {query} LIMIT 1"#,);
            self.fns.push(quote! {
                pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<bool> {
                    orm::sqlx::query_scalar!(#sql, #(#bindings),*)
                        .fetch_one(tx.connection()).await
                }
            });
        } else if fn_name_string.starts_with("get_id_") {
            if self.primary_keys.len() == 1 {
                let field = self.primary_keys.first().unwrap();
                let name = field.ident.as_ref().unwrap();
                let ty = &field.ty;
                let query = &format!(
                    r#"SELECT "{name}" as "{name}: _" FROM "{table_name}" {query} LIMIT 1"#,
                );
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<#ty>> {
                        orm::sqlx::query_scalar!(#query, #(#bindings),*)
                            .fetch_optional(tx.connection()).await
                    }
                });
            } else {
                let mut columns = Vec::new();
                let mut fields = Vec::new();
                let mut ret = Vec::new();
                let mut tys = Vec::new();
                for field in &self.primary_keys {
                    let name = field.ident.as_ref().unwrap();
                    let ty = &field.ty;
                    columns.push(format!(r#""{name}" as "{name}: _""#));
                    fields.push(quote! { #name: #ty });
                    ret.push(quote! { ids.#name });
                    tys.push(ty);
                }
                let columns = columns.join(", ");
                let query = &format!(r#"SELECT {columns} FROM "{table_name}" {query} LIMIT 1"#,);
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<(#(#tys),*)>> {
                        #[derive(sqlx::FromRow)]
                        struct Ids { #(#fields),* };
                        let ids = orm::sqlx::query_as!(Ids, #query, #(#bindings),*)
                            .fetch_optional(tx.connection()).await?;
                        Ok(ids.map(|ids| (#(#ret),*)))
                    }
                });
            }
        } else if fn_name_string.starts_with("get_") {
            let ret_span = ret.span();
            let (sql, ret, is_scalar) = match ret {
                ReturnType::Default => (
                    format!(r#"SELECT {column_names_typed} FROM "{table_name}" {query} LIMIT 1"#),
                    quote! { Self },
                    false,
                ),
                ReturnType::Type(_, ret) => {
                    let (ret, is_scalar) = match &*ret {
                        syn::Type::Path(_) => (quote! { #ret }, false),
                        syn::Type::Tuple(tuple) => {
                            if tuple.elems.len() == 1 {
                                let ret = tuple.elems.first().unwrap();
                                (quote! { #ret }, true)
                            } else {
                                (quote! { #ret }, true)
                            }
                        }
                        _ => {
                            fail!(ret_span, "Return type must be path or tuple");
                        }
                    };
                    (format!(r#"{query} LIMIT 1"#), ret, is_scalar)
                }
            };
            if is_scalar {
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<#ret>> {
                        orm::sqlx::query_scalar!(#sql, #(#bindings),*)
                            .fetch_optional(tx.connection()).await
                    }
                });
            } else {
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<#ret>> {
                        orm::sqlx::query_as!(#struct_name, #sql, #(#bindings),*)
                            .fetch_optional(tx.connection()).await
                    }
                });
            }
        } else if fn_name_string.starts_with("list_") {
            let ret_span = ret.span();
            let (sql, ret, is_scalar) = match ret {
                ReturnType::Default => (
                    format!(r#"SELECT {column_names_typed} FROM "{table_name}" {query}"#),
                    quote! { Self },
                    false,
                ),
                ReturnType::Type(_, ret) => {
                    let (ret, is_scalar) = match &*ret {
                        syn::Type::Path(_) => (quote! { #ret }, false),
                        syn::Type::Tuple(tuple) => {
                            if tuple.elems.len() == 1 {
                                let ret = tuple.elems.first().unwrap();
                                (quote! { #ret }, true)
                            } else {
                                (quote! { #ret }, true)
                            }
                        }
                        _ => {
                            fail!(ret_span, "Return type must be path or tuple");
                        }
                    };
                    (query, ret, is_scalar)
                }
            };
            if is_scalar {
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Vec<#ret>> {
                        orm::sqlx::query_scalar!(#sql, #(#bindings),*)
                            .fetch_all(tx.connection()).await
                    }
                });
            } else {
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Vec<#ret>> {
                        orm::sqlx::query_as!(#ret, #sql, #(#bindings),*)
                            .fetch_all(tx.connection()).await
                    }
                });
            }
        } else {
            fail!(
                fn_name.span(),
                format!("Unknown function prefix {fn_name_string}")
            );
        }
    }

    fn where_primary_keys_sql(&self, binding_offset: &mut usize) -> String {
        self.primary_keys
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().unwrap();
                *binding_offset += 1;
                format!(r#""{name}" = ${binding_offset}"#)
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

#[proc_macro_derive(Model, attributes(orm))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ast_span = ast.span();

    let Data::Struct(s) = &ast.data else {
        return Error::new(ast.span(), "Model can only be derived for structs")
            .to_compile_error()
            .into();
    };

    let Fields::Named(named_fields) = &s.fields else {
        return Error::new(ast.span(), "Model only supports structs with named fields")
            .to_compile_error()
            .into();
    };

    let struct_name = ast.ident.clone();
    let mut table_name = struct_name.to_string().to_case(Case::Snake);
    if !table_name.ends_with("es") {
        if table_name.ends_with("s") {
            table_name.push_str("es");
        } else {
            table_name.push('s');
        }
    }
    let fields: Vec<_> = named_fields.named.clone().into_iter().collect();
    let field_map: HashMap<_, _> = fields
        .clone()
        .into_iter()
        .map(|field| (field.ident.as_ref().unwrap().to_string(), field))
        .collect();

    let mut attrs = Vec::new();

    // determine primary keys

    let mut primary_keys = Vec::new();
    for attr in ast.attrs {
        let Ok(list) = attr.meta.require_list() else {
            return Error::new(attr.span(), "Invalid attribute format")
                .to_compile_error()
                .into();
        };
        let call = match list.parse_args::<syn::ExprCall>() {
            Ok(call) => call,
            Err(_) => {
                attrs.push(attr);
                continue;
            }
        };
        let Expr::Path(func) = call.func.as_ref() else {
            return Error::new(attr.span(), "Invalid attribute format")
                .to_compile_error()
                .into();
        };
        let fn_name = func.path.get_ident().unwrap();
        if fn_name == "primary_keys" {
            for arg in &call.args {
                let Expr::Path(path) = arg else {
                    return Error::new(attr.span(), "Invalid attribute format")
                        .to_compile_error()
                        .into();
                };
                let ident = path.path.get_ident().unwrap().clone();
                let Some(field) = field_map.get(&ident.to_string()) else {
                    return Error::new(
                        arg.span(),
                        format!("{struct_name} does not contain field {ident}"),
                    )
                    .to_compile_error()
                    .into();
                };
                primary_keys.push(field.clone());
            }
        }
    }
    if primary_keys.is_empty() {
        let Some(field) = field_map.get("id") else {
            return Error::new(
                ast_span,
                format!("expected {struct_name} to contain field id"),
            )
            .to_compile_error()
            .into();
        };
        primary_keys.push(field.clone());
    }

    let mut table = Table {
        fields,
        fns: Default::default(),
        placeholder_re: Regex::new(r"\$\d+").unwrap(),
        primary_keys_set: primary_keys.clone().into_iter().collect(),
        primary_keys,
        struct_name,
        table_name,
    };
    table.delete();
    table.get();
    table.insert();
    table.update();
    table.upsert();

    for attr in attrs {
        let Ok(list) = attr.meta.require_list() else {
            return Error::new(attr.span(), "Invalid attribute format")
                .to_compile_error()
                .into();
        };
        let Ok(item_fn) = list.parse_args::<syn::ItemFn>() else {
            return Error::new(attr.span(), "Expected valid function definition")
                .to_compile_error()
                .into();
        };
        table.userfn(item_fn);
    }

    let struct_name = table.struct_name;
    let fns = table.fns;
    TokenStream::from(quote! {
        impl #struct_name {
            #(#fns)*
        }
    })
}
