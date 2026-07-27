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
    let value_extractions = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            let #field_name = row.take(#db_column_name)?;
        }
    });

    let field_idents = fields
        .iter()
        .map(|f| f.ident.as_ref().expect("named field must have identifier"));

    // TODO(#5592): check that the schema and this struct have same columns/attributes count.

    let expanded = quote! {
        impl std::convert::TryFrom<google_cloud_bigquery::Row> for #name {
            type Error = google_cloud_bigquery::RowError;

            fn try_from(mut row: google_cloud_bigquery::Row) -> std::result::Result<Self, Self::Error> {
                #( #value_extractions )*

                std::result::Result::Ok(Self {
                    #( #field_idents, )*
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

    let field_idents_struct_array = fields
        .iter()
        .map(|f| f.ident.as_ref().expect("named field must have identifier"));
    let field_idents_struct_obj = fields
        .iter()
        .map(|f| f.ident.as_ref().expect("named field must have identifier"));

    let field_extractions_array = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            let #field_name = iter.next()
                .ok_or_else(|| google_cloud_bigquery::ConvertError::MissingField(#db_column_name.to_string()))?;
            let #field_name = google_cloud_bigquery::FromSql::from_sql(#field_name)?;
        }
    });

    let field_extractions_obj = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().expect("named field must have identifier");
        let db_column_name = get_field_name(f);
        quote! {
            let #field_name = obj.remove(#db_column_name)
                .ok_or_else(|| google_cloud_bigquery::ConvertError::MissingField(#db_column_name.to_string()))?;
            let #field_name = google_cloud_bigquery::FromSql::from_sql(#field_name)?;
        }
    });

    let expanded = quote! {
        impl google_cloud_bigquery::FromSql for #name {
            fn from_sql(value: wkt::Value) -> std::result::Result<Self, google_cloud_bigquery::ConvertError> {
                match value {
                    wkt::Value::Array(arr) => {
                        let mut iter = arr.into_iter();
                        #( #field_extractions_array )*
                        std::result::Result::Ok(Self {
                            #( #field_idents_struct_array, )*
                        })
                    }
                    wkt::Value::Object(mut obj) => {
                        #( #field_extractions_obj )*
                        std::result::Result::Ok(Self {
                            #( #field_idents_struct_obj, )*
                        })
                    }
                    other => std::result::Result::Err(google_cloud_bigquery::ConvertError::TypeMismatch {
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
