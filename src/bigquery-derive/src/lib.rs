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
/// Supports renaming attributes via `#[bigquery(rename = "new_name")]`.
///
/// [TryFrom]: std::convert::TryFrom
#[proc_macro_derive(FromRow, attributes(bigquery))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_row_impl(input).into()
}

fn derive_from_row_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) if !fields.named.is_empty() => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromRow can only be derived for non-empty structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromRow can only be derived for non-empty structs with named fields",
            )
            .to_compile_error();
        }
    };
    for f in &fields {
        if let Err(err) = get_field_name(f) {
            return err.to_compile_error();
        }
    }
    let field_initializations = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f).expect("validated above");
        quote! {
            #field_name: row.take(#db_column_name)?,
        }
    });

    // TODO(#5592): check that the schema and this struct have same columns/attributes count.

    quote! {
        impl std::convert::TryFrom<google_cloud_bigquery::query::Row> for #name {
            type Error = google_cloud_bigquery::error::RowError;

            fn try_from(mut row: google_cloud_bigquery::query::Row) -> std::result::Result<Self, Self::Error> {
                std::result::Result::Ok(Self {
                    #( #field_initializations )*
                })
            }
        }
    }
}

/// Derives `FromSql` for converting a BigQuery value into a struct.
///
/// Supports renaming attributes via `#[bigquery(rename = "new_name")]`.
#[proc_macro_derive(FromSql, attributes(bigquery))]
pub fn derive_from_sql(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_sql_impl(input).into()
}

fn derive_from_sql_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) if !fields.named.is_empty() => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromSql can only be derived for non-empty structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromSql can only be derived for non-empty structs with named fields",
            )
            .to_compile_error();
        }
    };

    for f in &fields {
        if let Err(err) = get_field_name(f) {
            return err.to_compile_error();
        }
    }

    let field_initializations = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f).expect("validated above");
        quote! {
            #field_name: value.take(#db_column_name)?,
        }
    });

    quote! {
        impl google_cloud_bigquery::query::FromSql for #name {
            fn from_value(mut value: google_cloud_bigquery::query::SqlValue) -> std::result::Result<Self, google_cloud_bigquery::error::ConvertError> {
                std::result::Result::Ok(Self {
                    #( #field_initializations )*
                })
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

    #[test]
    fn test_rejects_empty_named_structs() -> Result<(), syn::Error> {
        let row_err = derive_from_row_impl(syn::parse_str("struct Empty {}")?).to_string();
        assert!(
            row_err.contains("FromRow can only be derived for non-empty structs with named fields"),
            "unexpected expansion: {row_err}"
        );

        let sql_err = derive_from_sql_impl(syn::parse_str("struct Empty {}")?).to_string();
        assert!(
            sql_err.contains("FromSql can only be derived for non-empty structs with named fields"),
            "unexpected expansion: {sql_err}"
        );
        Ok(())
    }

    #[test]
    fn test_rejects_non_structs() -> Result<(), syn::Error> {
        let row_err = derive_from_row_impl(syn::parse_str("enum Foo {}")?).to_string();
        assert!(
            row_err.contains("FromRow can only be derived for non-empty structs with named fields"),
            "unexpected expansion: {row_err}"
        );

        let sql_err = derive_from_sql_impl(syn::parse_str("enum Foo {}")?).to_string();
        assert!(
            sql_err.contains("FromSql can only be derived for non-empty structs with named fields"),
            "unexpected expansion: {sql_err}"
        );
        Ok(())
    }
}
