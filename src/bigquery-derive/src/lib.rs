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
use quote::{quote, quote_spanned};
use std::collections::HashSet;
use syn::spanned::Spanned;
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
                if let Err(err) = reject_bigquery_attrs(&fields.unnamed) {
                    return err.to_compile_error();
                }
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

    let generics = add_trait_bounds(
        input.generics,
        quote!(google_cloud_bigquery::query::FromSql),
    );
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics std::convert::TryFrom<google_cloud_bigquery::query::Row> for #name #ty_generics #where_clause {
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
                if let Err(err) = reject_bigquery_attrs(&fields.unnamed) {
                    return err.to_compile_error();
                }
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

    let generics = add_trait_bounds(
        input.generics,
        quote!(google_cloud_bigquery::query::FromSql),
    );
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics google_cloud_bigquery::query::FromSql for #name #ty_generics #where_clause {
            fn from_value(mut value: google_cloud_bigquery::query::SqlValue) -> std::result::Result<Self, google_cloud_bigquery::error::ConvertError> {
                std::result::Result::Ok(#body)
            }
        }
    }
}

/// Derives `ToRow` for writing a struct as rows with the BigQuery `Write` client.
///
/// Each field is written to the column with the same name (or via `#[bigquery(rename = "new_name")]`).
/// Only structs with named fields are supported. A struct with `#[derive(ToRow)]` can also be a
/// field in another one, for a `STRUCT` column.
#[proc_macro_derive(ToRow, attributes(bigquery))]
pub fn derive_to_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_to_row_impl(input).into()
}

fn derive_to_row_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    if let Err(err) = reject_struct_bigquery_attrs(&input.attrs) {
        return err.to_compile_error();
    }
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(syn::DataStruct {
            fields: Fields::Named(fields),
            ..
        }) if !fields.named.is_empty() => fields.named,
        _ => {
            return syn::Error::new_spanned(
                name,
                "ToRow can only be derived for non-empty structs with named fields",
            )
            .to_compile_error();
        }
    };

    // Describe and encode the fields in declaration order. The library numbers
    // the fields in the order of the calls, so the schema and the encoder
    // always agree.
    let mut descriptors = Vec::new();
    let mut encoders = Vec::new();
    let mut columns = HashSet::new();
    for field in &fields {
        let column = match field_column(field)
            .and_then(|column| check_to_row_column(column, &mut columns))
        {
            Ok(column) => column.name,
            Err(err) => return err.to_compile_error(),
        };
        let field_name = field
            .ident
            .as_ref()
            .expect("named field must have identifier");
        let field_type = &field.ty;
        // The calls keep the hygiene of `#[derive(ToRow)]`, but take the
        // location of the field's type. If the type does not implement
        // `ProtoValue`, the compiler reports the error at the field, instead
        // of at `#[derive(ToRow)]`.
        let span = proc_macro2::Span::call_site().located_at(field_type.span());
        descriptors.push(quote_spanned! {span=>
            fields.field::<#field_type>(#column);
        });
        encoders.push(quote_spanned! {span=>
            fields.field(&self.#field_name)?;
        });
    }

    let generics = add_trait_bounds(
        input.generics,
        quote!(google_cloud_bigquery::write::ProtoValue),
    );
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // The library describes each nested struct type once, and tells them apart
    // by their `TypeId`, so a struct must be `'static` to be a field. Rows need
    // no such bound.
    let predicates: Vec<_> = where_clause
        .map(|clause| clause.predicates.iter().collect())
        .unwrap_or_default();

    // `ToRow` describes and encodes the fields, for rows. `ProtoValue` and
    // `ProtoElement` let a field, or the elements of a `Vec<T>` field, hold
    // this struct.
    quote! {
        impl #impl_generics google_cloud_bigquery::write::ToRow for #name #ty_generics #where_clause {
            fn describe_fields(fields: &mut google_cloud_bigquery::write::StructSchema<'_>) {
                #( #descriptors )*
            }

            fn encode_fields(&self, fields: &mut google_cloud_bigquery::write::StructEncoder<'_>) -> std::result::Result<(), google_cloud_bigquery::error::ConvertError> {
                #( #encoders )*
                std::result::Result::Ok(())
            }
        }

        impl #impl_generics google_cloud_bigquery::write::ProtoValue for #name #ty_generics where #( #predicates, )* Self: 'static {
            fn describe(field: google_cloud_bigquery::write::FieldSchema<'_>) {
                field.message::<Self>();
            }

            fn encode(&self, field: google_cloud_bigquery::write::FieldEncoder<'_>) -> std::result::Result<(), google_cloud_bigquery::error::ConvertError> {
                field.message(self)
            }
        }

        impl #impl_generics google_cloud_bigquery::write::ProtoElement for #name #ty_generics where #( #predicates, )* Self: 'static {}
    }
}

fn reject_bigquery_attrs<'a>(fields: impl IntoIterator<Item = &'a syn::Field>) -> syn::Result<()> {
    for field in fields {
        if let Some(attr) = field.attrs.iter().find(|a| a.path().is_ident("bigquery")) {
            return Err(syn::Error::new_spanned(
                attr,
                "bigquery attributes are not supported on tuple struct fields",
            ));
        }
    }
    Ok(())
}

/// Adds `bound` to each generic type parameter.
///
/// The bound is a token stream, parsed once for each parameter. Cloning a
/// `syn::TypeParamBound` needs syn's `clone-impls` feature, which this crate
/// does not enable.
fn add_trait_bounds(mut generics: syn::Generics, bound: proc_macro2::TokenStream) -> syn::Generics {
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(#bound));
        }
    }
    generics
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

/// The column for a named field.
struct Column {
    /// The column name: the `rename` in the field's `#[bigquery(...)]`
    /// attributes, or else the field's name without any `r#` prefix.
    name: String,
    /// Where the name comes from: the `rename` value, or the field's name.
    span: proc_macro2::Span,
}

/// Returns the column for a named field, for `ToRow`.
///
/// Every `#[bigquery(...)]` attribute on the field is checked. An attribute
/// with no arguments, an unknown argument, or a second `rename` is an error.
fn field_column(field: &syn::Field) -> syn::Result<Column> {
    let mut rename: Option<syn::LitStr> = None;
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("bigquery")) {
        let mut empty = true;
        attr.parse_nested_meta(|meta| {
            empty = false;
            if !meta.path.is_ident("rename") {
                return Err(meta.error("unsupported bigquery attribute"));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            if rename.is_some() {
                return Err(meta.error("duplicate bigquery attribute `rename`"));
            }
            rename = Some(value);
            Ok(())
        })?;
        if empty {
            return Err(syn::Error::new_spanned(
                attr,
                "empty bigquery attribute, expected `rename = \"...\"`",
            ));
        }
    }
    if let Some(value) = rename {
        return Ok(Column {
            name: value.value(),
            span: value.span(),
        });
    }
    let ident = field
        .ident
        .as_ref()
        .expect("named field must have identifier");
    Ok(Column {
        name: syn::ext::IdentExt::unraw(ident).to_string(),
        span: ident.span(),
    })
}

/// Checks a column name for `ToRow`, and that no earlier field writes to the
/// same column. `columns` holds the earlier columns, in lowercase.
///
/// The column name is also the name of a protobuf field, so it must start
/// with an ASCII letter or `_`, and contain only ASCII letters, digits, and
/// `_`. BigQuery column names are case-insensitive, so `a` and `A` are the
/// same column.
fn check_to_row_column(column: Column, columns: &mut HashSet<String>) -> syn::Result<Column> {
    let mut chars = column.name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(syn::Error::new(
            column.span,
            format!(
                "`{}` is not a supported column name for ToRow: column names must start with an ASCII letter or `_`, and contain only ASCII letters, digits, and `_`",
                column.name
            ),
        ));
    }
    if !columns.insert(column.name.to_ascii_lowercase()) {
        return Err(syn::Error::new(
            column.span,
            format!(
                "duplicate column name `{}`: another field writes to it, and BigQuery column names are case-insensitive",
                column.name
            ),
        ));
    }
    Ok(column)
}

/// Returns an error for a `#[bigquery(...)]` attribute on the struct itself,
/// which only fields support.
fn reject_struct_bigquery_attrs(attrs: &[syn::Attribute]) -> syn::Result<()> {
    match attrs.iter().find(|a| a.path().is_ident("bigquery")) {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            "bigquery attributes are only supported on fields",
        )),
        None => Ok(()),
    }
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

        let to_row_tokens = derive_to_row_impl(make_input()).to_string();
        assert!(to_row_tokens.contains("unsupported bigquery attribute"));
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

    #[test_case(
        r#"struct Row { #[bigquery(rename = "a")] #[bigquery(rename = "b")] field: i64 }"#,
        "duplicate bigquery attribute `rename`";
        "rename in two attributes"
    )]
    #[test_case(
        r#"struct Row { #[bigquery(rename = "a", rename = "b")] field: i64 }"#,
        "duplicate bigquery attribute `rename`";
        "rename twice in one attribute"
    )]
    #[test_case(
        r#"struct Row { #[bigquery(rename = "a")] #[bigquery(renam = "b")] field: i64 }"#,
        "unsupported bigquery attribute";
        "invalid attribute after a valid one"
    )]
    #[test_case(
        "struct Row { #[bigquery()] field: i64 }",
        "empty bigquery attribute";
        "empty attribute"
    )]
    #[test_case(
        r#"#[bigquery(rename_all = "camelCase")] struct Row { field: i64 }"#,
        "bigquery attributes are only supported on fields";
        "struct attribute"
    )]
    #[test_case(
        r#"#[bigquery(rename = "custom")] struct Row(i64);"#,
        "bigquery attributes are only supported on fields";
        "tuple struct attribute"
    )]
    fn test_invalid_bigquery_attributes(def: &str, want: &str) -> Result<(), syn::Error> {
        let to_row = derive_to_row_impl(syn::parse_str(def)?).to_string();
        assert!(to_row.contains(want), "ToRow for {def}: {to_row}");
        Ok(())
    }

    #[test]
    fn test_rename_in_a_later_attribute() {
        let field = extract_first_field(parse_quote! {
            struct MyRow {
                #[doc = "Other attributes come first."]
                #[bigquery(rename = "custom_col")]
                field: i64,
            }
        });

        let column = field_column(&field).ok().map(|column| column.name);
        assert_eq!(column.as_deref(), Some("custom_col"));
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
    fn test_rejects_bigquery_attribute_on_tuple_struct_field() -> Result<(), syn::Error> {
        let def = r#"struct TupleWithAttr(#[bigquery(rename = "custom")] i64);"#;
        let row_err = derive_from_row_impl(syn::parse_str(def)?).to_string();
        assert!(
            row_err.contains("bigquery attributes are not supported on tuple struct fields"),
            "unexpected expansion: {row_err}"
        );

        let sql_err = derive_from_sql_impl(syn::parse_str(def)?).to_string();
        assert!(
            sql_err.contains("bigquery attributes are not supported on tuple struct fields"),
            "unexpected expansion: {sql_err}"
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

    #[test]
    fn test_generics_expansion() -> Result<(), syn::Error> {
        let input1: DeriveInput = syn::parse_str("struct Wrapper<T> { val: T }")?;
        let row_tokens = derive_from_row_impl(input1).to_string();
        assert!(
            row_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > std :: convert :: TryFrom < google_cloud_bigquery :: query :: Row > for Wrapper < T >"),
            "unexpected row expansion: {row_tokens}"
        );

        let input2: DeriveInput = syn::parse_str("struct Wrapper<T> { val: T }")?;
        let sql_tokens = derive_from_sql_impl(input2).to_string();
        assert!(
            sql_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > google_cloud_bigquery :: query :: FromSql for Wrapper < T >"),
            "unexpected sql expansion: {sql_tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_generics_expansion_with_where_clause() -> Result<(), syn::Error> {
        let input1: DeriveInput =
            syn::parse_str("struct Wrapper<T> where T: std::fmt::Debug { val: T }")?;
        let row_tokens = derive_from_row_impl(input1).to_string();
        assert!(
            row_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > std :: convert :: TryFrom < google_cloud_bigquery :: query :: Row > for Wrapper < T > where T : std :: fmt :: Debug"),
            "unexpected row expansion: {row_tokens}"
        );

        let input2: DeriveInput =
            syn::parse_str("struct Wrapper<T> where T: std::fmt::Debug { val: T }")?;
        let sql_tokens = derive_from_sql_impl(input2).to_string();
        assert!(
            sql_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > google_cloud_bigquery :: query :: FromSql for Wrapper < T > where T : std :: fmt :: Debug"),
            "unexpected sql expansion: {sql_tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_generics_expansion_with_default_type_param() -> Result<(), syn::Error> {
        let input1: DeriveInput = syn::parse_str("struct Wrapper<T = i64> { val: T }")?;
        let row_tokens = derive_from_row_impl(input1).to_string();
        assert!(
            row_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > std :: convert :: TryFrom < google_cloud_bigquery :: query :: Row > for Wrapper < T >"),
            "unexpected row expansion: {row_tokens}"
        );

        let input2: DeriveInput = syn::parse_str("struct Wrapper<T = i64> { val: T }")?;
        let sql_tokens = derive_from_sql_impl(input2).to_string();
        assert!(
            sql_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql > google_cloud_bigquery :: query :: FromSql for Wrapper < T >"),
            "unexpected sql expansion: {sql_tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_generics_expansion_multiple_params() -> Result<(), syn::Error> {
        let input1: DeriveInput = syn::parse_str("struct Pair<A, B> { a: A, b: B }")?;
        let row_tokens = derive_from_row_impl(input1).to_string();
        assert!(
            row_tokens.contains("impl < A : google_cloud_bigquery :: query :: FromSql , B : google_cloud_bigquery :: query :: FromSql > std :: convert :: TryFrom < google_cloud_bigquery :: query :: Row > for Pair < A , B >"),
            "unexpected row expansion: {row_tokens}"
        );

        let input2: DeriveInput = syn::parse_str("struct Pair<A, B> { a: A, b: B }")?;
        let sql_tokens = derive_from_sql_impl(input2).to_string();
        assert!(
            sql_tokens.contains("impl < A : google_cloud_bigquery :: query :: FromSql , B : google_cloud_bigquery :: query :: FromSql > google_cloud_bigquery :: query :: FromSql for Pair < A , B >"),
            "unexpected sql expansion: {sql_tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_generics_tuple_struct_expansion() -> Result<(), syn::Error> {
        let input1: DeriveInput = syn::parse_str("struct TupleWrapper<T, U>(T, U);")?;
        let row_tokens = derive_from_row_impl(input1).to_string();
        assert!(
            row_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql , U : google_cloud_bigquery :: query :: FromSql > std :: convert :: TryFrom < google_cloud_bigquery :: query :: Row > for TupleWrapper < T , U >"),
            "unexpected row expansion: {row_tokens}"
        );

        let input2: DeriveInput = syn::parse_str("struct TupleWrapper<T, U>(T, U);")?;
        let sql_tokens = derive_from_sql_impl(input2).to_string();
        assert!(
            sql_tokens.contains("impl < T : google_cloud_bigquery :: query :: FromSql , U : google_cloud_bigquery :: query :: FromSql > google_cloud_bigquery :: query :: FromSql for TupleWrapper < T , U >"),
            "unexpected sql expansion: {sql_tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_to_row_expansion() {
        let input: DeriveInput = parse_quote! {
            struct Row {
                name: String,
                #[bigquery(rename = "family_name")]
                surname: String,
                r#type: String,
            }
        };
        // The fields are described and encoded in declaration order. Renamed
        // fields use the new name, and raw identifiers lose their `r#` prefix.
        let want = quote! {
            impl google_cloud_bigquery::write::ToRow for Row {
                fn describe_fields(fields: &mut google_cloud_bigquery::write::StructSchema<'_>) {
                    fields.field::<String>("name");
                    fields.field::<String>("family_name");
                    fields.field::<String>("type");
                }

                fn encode_fields(&self, fields: &mut google_cloud_bigquery::write::StructEncoder<'_>) -> std::result::Result<(), google_cloud_bigquery::error::ConvertError> {
                    fields.field(&self.name)?;
                    fields.field(&self.surname)?;
                    fields.field(&self.r#type)?;
                    std::result::Result::Ok(())
                }
            }

            impl google_cloud_bigquery::write::ProtoValue for Row where Self: 'static {
                fn describe(field: google_cloud_bigquery::write::FieldSchema<'_>) {
                    field.message::<Self>();
                }

                fn encode(&self, field: google_cloud_bigquery::write::FieldEncoder<'_>) -> std::result::Result<(), google_cloud_bigquery::error::ConvertError> {
                    field.message(self)
                }
            }

            impl google_cloud_bigquery::write::ProtoElement for Row where Self: 'static {}
        };
        assert_eq!(derive_to_row_impl(input).to_string(), want.to_string());
    }

    #[test_case("struct Empty {}"; "empty named struct")]
    #[test_case("struct Tuple(String);"; "tuple struct")]
    #[test_case("struct Unit;"; "unit struct")]
    #[test_case("enum Foo { A }"; "enum")]
    #[test_case("union Bits { a: u32 }"; "union")]
    fn test_to_row_requires_named_fields(def: &str) -> Result<(), syn::Error> {
        let err = derive_to_row_impl(syn::parse_str(def)?).to_string();
        assert!(
            err.contains("ToRow can only be derived for non-empty structs with named fields"),
            "unexpected expansion for {def}: {err}"
        );
        Ok(())
    }

    #[test_case("google_cloud_bigquery :: write :: ToRow"; "row")]
    #[test_case("google_cloud_bigquery :: write :: ProtoValue"; "value")]
    #[test_case("google_cloud_bigquery :: write :: ProtoElement"; "element")]
    fn test_to_row_generics_expansion(trait_path: &str) -> Result<(), syn::Error> {
        let input: DeriveInput = syn::parse_str("struct Wrapper<T> { val: T }")?;
        let tokens = derive_to_row_impl(input).to_string();
        let want = format!(
            "impl < T : google_cloud_bigquery :: write :: ProtoValue > {trait_path} for Wrapper < T >"
        );
        assert!(tokens.contains(&want), "unexpected expansion: {tokens}");
        Ok(())
    }

    #[test_case("google_cloud_bigquery :: write :: ToRow", "where T : Clone {"; "row")]
    #[test_case("google_cloud_bigquery :: write :: ProtoValue", "where T : Clone , Self : 'static {"; "value")]
    #[test_case("google_cloud_bigquery :: write :: ProtoElement", "where T : Clone , Self : 'static {"; "element")]
    fn test_to_row_where_clause_expansion(
        trait_path: &str,
        where_clause: &str,
    ) -> Result<(), syn::Error> {
        let input: DeriveInput = syn::parse_str("struct Wrapper<T> where T: Clone { val: T }")?;
        let tokens = derive_to_row_impl(input).to_string();
        // Each impl keeps the struct's own `where` clause. Only the impls for
        // fields add `Self: 'static`.
        let want = format!(
            "impl < T : google_cloud_bigquery :: write :: ProtoValue > {trait_path} for Wrapper < T > {where_clause}"
        );
        assert!(tokens.contains(&want), "unexpected expansion: {tokens}");
        Ok(())
    }

    #[test_case(r#"struct Row { #[bigquery(rename = "my-col")] field: i64 }"#, "`my-col`"; "dash")]
    #[test_case(r#"struct Row { #[bigquery(rename = "my col")] field: i64 }"#, "`my col`"; "space")]
    #[test_case(r#"struct Row { #[bigquery(rename = "0col")] field: i64 }"#, "`0col`"; "leading digit")]
    #[test_case(r#"struct Row { #[bigquery(rename = "")] field: i64 }"#, "``"; "empty")]
    #[test_case("struct Row { café: i64 }", "`café`"; "non-ascii field name")]
    fn test_to_row_rejects_column_names(def: &str, name: &str) -> Result<(), syn::Error> {
        let tokens = derive_to_row_impl(syn::parse_str(def)?).to_string();
        let want = format!("{name} is not a supported column name for ToRow");
        assert!(
            tokens.contains(&want),
            "unexpected expansion for {def}: {tokens}"
        );
        // The read side has no such limit.
        let tokens = derive_from_row_impl(syn::parse_str(def)?).to_string();
        assert!(!tokens.contains("compile_error"), "{tokens}");
        Ok(())
    }

    #[test_case(r#"struct Row { a: i64, #[bigquery(rename = "a")] b: i64 }"#, "`a`"; "same name")]
    #[test_case(r#"struct Row { name: i64, #[bigquery(rename = "NAME")] other: i64 }"#, "`NAME`"; "different case")]
    fn test_to_row_rejects_duplicate_columns(def: &str, name: &str) -> Result<(), syn::Error> {
        let tokens = derive_to_row_impl(syn::parse_str(def)?).to_string();
        let want = format!("duplicate column name {name}");
        assert!(
            tokens.contains(&want),
            "unexpected expansion for {def}: {tokens}"
        );
        Ok(())
    }

    #[test]
    fn test_to_row_accepts_column_names() -> Result<(), syn::Error> {
        let def =
            r#"struct Row { _private: i64, r#type: i64, #[bigquery(rename = "Col_2")] col: i64 }"#;
        let tokens = derive_to_row_impl(syn::parse_str(def)?).to_string();
        assert!(!tokens.contains("compile_error"), "{tokens}");
        Ok(())
    }
}
