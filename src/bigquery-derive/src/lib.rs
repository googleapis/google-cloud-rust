// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Derive macros for the Google Cloud BigQuery client.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derives standard library [TryFrom] for converting a BigQuery `Row` into a struct.
///
/// Structs with named fields match columns by field name (or via `#[bigquery(rename = "new_name")]`).
/// Tuple structs match columns positionally by 0-based index.
///
/// [TryFrom]: std::convert::TryFrom
#[proc_macro_derive(FromRow, attributes(bigquery))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_row_impl(input).into()
}

fn derive_from_row_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;

    let body = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) if !fields.named.is_empty() => {
                for f in &fields.named {
                    if let Err(err) = get_field_name(f) {
                        return err.to_compile_error();
                    }
                }
                let field_initializations = fields.named.iter().map(|f| {
                    let field_name = f.ident.as_ref().expect("named field must have identifier");
                    let db_column_name = get_field_name(f).expect("validated above");
                    quote! {
                        #field_name: row.take(#db_column_name)?,
                    }
                });
                quote! {
                    Self {
                        #( #field_initializations )*
                    }
                }
            }
            Fields::Unnamed(fields) if !fields.unnamed.is_empty() => {
                let field_initializations = (0..fields.unnamed.len()).map(|idx| {
                    quote! {
                        row.take(#idx)?,
                    }
                });
                quote! {
                    Self(
                        #( #field_initializations )*
                    )
                }
            }
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromRow can only be derived for non-empty structs",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromRow can only be derived for non-empty structs",
            )
            .to_compile_error();
        }
    };

    // TODO(#5592): check that the schema and this struct have same columns/attributes count.

    quote! {
        impl std::convert::TryFrom<google_cloud_bigquery::query::Row> for #name {
            type Error = google_cloud_bigquery::error::RowError;

            fn try_from(mut row: google_cloud_bigquery::query::Row) -> std::result::Result<Self, Self::Error> {
                std::result::Result::Ok(#body)
            }
        }
    }
}

/// Derives `FromSql` for converting a BigQuery `STRUCT` value into a Rust struct.
///
/// Structs with named fields match `STRUCT` fields by field name (or via `#[bigquery(rename = "new_name")]`).
/// Tuple structs match `STRUCT` fields positionally by 0-based index, supporting anonymous
/// `STRUCT(1, 'a')` fields and ordered positional extraction.
#[proc_macro_derive(FromSql, attributes(bigquery))]
pub fn derive_from_sql(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_sql_impl(input).into()
}

fn derive_from_sql_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;

    let body = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) if !fields.named.is_empty() => {
                for f in &fields.named {
                    if let Err(err) = get_field_name(f) {
                        return err.to_compile_error();
                    }
                }
                let field_initializations = fields.named.iter().map(|f| {
                    let field_name = f.ident.as_ref().expect("named field must have identifier");
                    let db_column_name = get_field_name(f).expect("validated above");
                    quote! {
                        #field_name: value.take(#db_column_name)?,
                    }
                });
                quote! {
                    Self {
                        #( #field_initializations )*
                    }
                }
            }
            Fields::Unnamed(fields) if !fields.unnamed.is_empty() => {
                let field_initializations = (0..fields.unnamed.len()).map(|idx| {
                    quote! {
                        value.take(#idx)?,
                    }
                });
                quote! {
                    Self(
                        #( #field_initializations )*
                    )
                }
            }
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromSql can only be derived for non-empty structs",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromSql can only be derived for non-empty structs",
            )
            .to_compile_error();
        }
    };

    quote! {
        impl google_cloud_bigquery::query::FromSql for #name {
            fn from_value(mut value: google_cloud_bigquery::query::SqlValue) -> std::result::Result<Self, google_cloud_bigquery::error::ConvertError> {
                std::result::Result::Ok(#body)
            }
        }
    }
}

fn get_field_name(field: &syn::Field) -> syn::Result<String> {
    for attr in &field.attrs {
        if attr.path().is_ident("bigquery") {
            let mut renamed = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    renamed = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("unsupported bigquery attribute"))
                }
            })?;
            if let Some(name) = renamed {
                return Ok(name);
            }
        }
    }
    Ok(syn::ext::IdentExt::unraw(
        field
            .ident
            .as_ref()
            .expect("named field must have identifier"),
    )
    .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;
    use test_case::test_case;

    fn extract_first_field(input: DeriveInput) -> syn::Field {
        match input.data {
            Data::Struct(s) => match s.fields {
                Fields::Named(n) => n.named.into_iter().next().unwrap(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_invalid_bigquery_attribute_typo_errors() {
        let make_input = || -> DeriveInput {
            parse_quote! {
                struct MyRow {
                    #[bigquery(renam = "custom_col")]
                    field: i64,
                }
            }
        };
        let field = extract_first_field(make_input());

        let err = get_field_name(&field).unwrap_err();
        assert!(
            err.to_string().contains("unsupported bigquery attribute"),
            "{err}"
        );

        let row_tokens = derive_from_row_impl(make_input()).to_string();
        assert!(row_tokens.contains("unsupported bigquery attribute"));

        let sql_tokens = derive_from_sql_impl(make_input()).to_string();
        assert!(sql_tokens.contains("unsupported bigquery attribute"));
    }

    #[test]
    fn test_invalid_bigquery_attribute_non_string_value_errors() {
        let field = extract_first_field(parse_quote! {
            struct MyRow {
                #[bigquery(rename = 123)]
                field: i64,
            }
        });

        assert!(get_field_name(&field).is_err());
    }

    #[test_case("struct Empty {}"; "empty named struct")]
    #[test_case("struct EmptyTuple();"; "empty tuple struct")]
    #[test_case("struct Unit;"; "unit struct")]
    fn test_rejects_empty_structs(def: &str) -> Result<(), syn::Error> {
        let row_err = derive_from_row_impl(syn::parse_str(def)?).to_string();
        assert!(
            row_err.contains("FromRow can only be derived for non-empty structs"),
            "unexpected expansion for {def}: {row_err}"
        );

        let sql_err = derive_from_sql_impl(syn::parse_str(def)?).to_string();
        assert!(
            sql_err.contains("FromSql can only be derived for non-empty structs"),
            "unexpected expansion for {def}: {sql_err}"
        );
        Ok(())
    }

    #[test]
    fn test_rejects_non_structs() -> Result<(), syn::Error> {
        let row_err = derive_from_row_impl(syn::parse_str("enum Foo {}")?).to_string();
        assert!(
            row_err.contains("FromRow can only be derived for non-empty structs"),
            "unexpected expansion: {row_err}"
        );

        let sql_err = derive_from_sql_impl(syn::parse_str("enum Foo {}")?).to_string();
        assert!(
            sql_err.contains("FromSql can only be derived for non-empty structs"),
            "unexpected expansion: {sql_err}"
        );
        Ok(())
    }
}
