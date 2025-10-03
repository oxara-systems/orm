use std::collections::{BTreeSet, HashMap, HashSet};

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use regex::Regex;
use syn::{
    parse_macro_input, parse_quote, punctuated::Punctuated, spanned::Spanned, token::Comma, Data,
    DeriveInput, Error, Expr, Field, Fields, FnArg, Ident, Lit,
};

enum FnKind {
    Get,
    GetId,
    Is,
    List,
}

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

    fn userfn(&mut self, fn_kind: FnKind, fn_name: Ident, args: Punctuated<Expr, Comma>) {
        let mut args_iter = args.iter();
        let query = args_iter.next().unwrap();
        let Expr::Lit(query) = query else {
            self.fns
                .push(Error::new(query.span(), "expected query string literal").to_compile_error());
            return;
        };
        let Lit::Str(query) = &query.lit else {
            self.fns
                .push(Error::new(query.span(), "expected query string literal").to_compile_error());
            return;
        };
        let query = query.value();

        let struct_name = &self.struct_name;
        let table_name = &self.table_name;
        let column_names_typed = self.column_names_typed_sql();

        let mut params: Vec<FnArg> = Vec::new();
        let mut bindings = Vec::new();
        for arg in args_iter {
            let Expr::Tuple(tuple) = &arg else {
                self.fns.push(
                    Error::new(
                        arg.span(),
                        "expected tuple containing function parameter in the form of: (name, Type)",
                    )
                    .to_compile_error(),
                );
                return;
            };
            let mut iter = tuple.elems.iter();
            let Some(name) = iter.next() else {
                self.fns
                    .push(Error::new(arg.span(), "expected parameter name").to_compile_error());
                return;
            };
            let Some(ty) = iter.next() else {
                self.fns
                    .push(Error::new(arg.span(), "expected parameter type").to_compile_error());
                return;
            };
            params.push(parse_quote! { #name: &#ty });
            bindings.push(quote! { #name as _ });
        }

        let placeholders: BTreeSet<_> = self
            .placeholder_re
            .find_iter(&query)
            .map(|x| x.as_str().to_string())
            .collect();
        if placeholders.len() != params.len() {
            self.fns.push(
                Error::new(
                    args.span(),
                    format!(
                        "{} expects {} parameters, but has {}",
                        fn_name,
                        placeholders.len(),
                        params.len()
                    ),
                )
                .to_compile_error(),
            );
            return;
        }

        match fn_kind {
            FnKind::Get => {
                let sql =
                    &format!(r#"SELECT {column_names_typed} FROM "{table_name}" {query} LIMIT 1"#,);
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Option<Self>> {
                        orm::sqlx::query_as!(#struct_name, #sql, #(#bindings),*)
                            .fetch_optional(tx.connection()).await
                    }
                });
            }
            FnKind::GetId => {
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
                    let query =
                        &format!(r#"SELECT {columns} FROM "{table_name}" {query} LIMIT 1"#,);
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
            }
            FnKind::Is => {
                let sql = &format!(
                    r#"SELECT COUNT(*) > 0 AS "bool!" FROM "{table_name}" {query} LIMIT 1"#,
                );
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<bool> {
                        orm::sqlx::query_scalar!(#sql, #(#bindings),*)
                            .fetch_one(tx.connection()).await
                    }
                });
            }
            FnKind::List => {
                let sql = &format!(r#"SELECT {column_names_typed} FROM "{table_name}" {query}"#,);
                self.fns.push(quote! {
                    pub async fn #fn_name(tx: &mut impl orm::ReadableTransaction, #(#params),*) -> orm::sqlx::Result<Vec<Self>> {
                        orm::sqlx::query_as!(#struct_name, #sql, #(#bindings),*)
                            .fetch_all(tx.connection()).await
                    }
                });
            }
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
    table_name.push('s');
    let fields: Vec<_> = named_fields.named.clone().into_iter().collect();
    let field_map: HashMap<_, _> = fields
        .clone()
        .into_iter()
        .map(|field| (field.ident.as_ref().unwrap().to_string(), field))
        .collect();

    // determine primary keys

    let mut primary_keys = Vec::new();
    for attr in &ast.attrs {
        if !attr.path().is_ident("orm") {
            continue;
        }
        let Ok(list) = attr.meta.require_list() else {
            return Error::new(attr.span(), "Invalid attribute format")
                .to_compile_error()
                .into();
        };
        let Ok(call) = list.parse_args::<syn::ExprCall>() else {
            return Error::new(attr.span(), "Invalid attribute format")
                .to_compile_error()
                .into();
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
                ast.span(),
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

    for attr in &ast.attrs {
        if !attr.path().is_ident("orm") {
            continue;
        }
        let list = attr.meta.require_list().unwrap();
        let call: syn::ExprCall = list.parse_args().unwrap();
        let Expr::Path(func) = call.func.as_ref() else {
            panic!("expected function call");
        };
        let fn_name = func.path.get_ident().unwrap().clone();
        let fn_name_string = fn_name.to_string();

        if fn_name_string.starts_with("get_id_") {
            table.userfn(FnKind::GetId, fn_name, call.args);
        } else if fn_name_string.starts_with("get_") {
            table.userfn(FnKind::Get, fn_name, call.args);
        } else if fn_name_string.starts_with("is_") {
            table.userfn(FnKind::Is, fn_name, call.args);
        } else if fn_name_string.starts_with("list_") {
            table.userfn(FnKind::List, fn_name, call.args);
        } else if fn_name == "primary_keys" {
            continue;
        } else {
            return Error::new(
                fn_name.span(),
                format!("Unknown attribute {fn_name_string}"),
            )
            .to_compile_error()
            .into();
        }
    }

    let struct_name = table.struct_name;
    let fns = table.fns;
    TokenStream::from(quote! {
        impl #struct_name {
            #(#fns)*
        }
    })
}
