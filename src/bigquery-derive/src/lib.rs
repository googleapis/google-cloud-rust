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
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromRow can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "FromRow can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };
    let field_initializations = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            #field_name: row.take(#db_column_name)?,
        }
    });

    // TODO(#5592): check that the schema and this struct have same columns/attributes count.

    let expanded = quote! {
        impl std::convert::TryFrom<google_cloud_bigquery::query::Row> for #name {
            type Error = google_cloud_bigquery::error::RowError;

            fn try_from(mut row: google_cloud_bigquery::query::Row) -> std::result::Result<Self, Self::Error> {
                std::result::Result::Ok(Self {
                    #( #field_initializations )*
                })
            }
        }
    };

    expanded.into()
}

/// Derives `FromSql` for converting a BigQuery value into a struct.
///
/// Supports renaming attributes via `#[bigquery(rename = "new_name")]`.
#[proc_macro_derive(FromSql, attributes(bigquery))]
pub fn derive_from_sql(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromSql can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "FromSql can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let field_initializations_array = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            #field_name: {
                let val = iter.next()
                    .ok_or_else(|| google_cloud_bigquery::error::ConvertError::MissingField(#db_column_name.to_string()))?;
                google_cloud_bigquery::query::FromSql::from_value(val)?
            },
        }
    });

    let field_initializations_obj = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            #field_name: {
                let val = obj.remove(#db_column_name)
                    .ok_or_else(|| google_cloud_bigquery::error::ConvertError::MissingField(#db_column_name.to_string()))?;
                google_cloud_bigquery::query::FromSql::from_value(val)?
            },
        }
    });

    let expanded = quote! {
        impl google_cloud_bigquery::query::FromSql for #name {
            fn from_value(value: wkt::Value) -> std::result::Result<Self, google_cloud_bigquery::error::ConvertError> {
                match value {
                    wkt::Value::Array(arr) => {
                        let mut iter = arr.into_iter();
                        std::result::Result::Ok(Self {
                            #( #field_initializations_array )*
                        })
                    }
                    wkt::Value::Object(mut obj) => {
                        std::result::Result::Ok(Self {
                            #( #field_initializations_obj )*
                        })
                    }
                    other => std::result::Result::Err(google_cloud_bigquery::error::ConvertError::TypeMismatch {
                        expected: "array or object",
                        got: other,
                    }),
                }
            }
        }
    };

    expanded.into()
}

fn get_field_name(field: &syn::Field) -> String {
    for attr in &field.attrs {
        if attr.path().is_ident("bigquery") {
            let mut renamed = None;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    renamed = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("unsupported bigquery attribute"))
                }
            });
            if let Some(name) = renamed {
                return name;
            }
        }
    }
    field
        .ident
        .as_ref()
        .expect("named field must have identifier")
        .to_string()
}
