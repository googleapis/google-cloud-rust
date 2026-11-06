// Copyright 2025 Google LLC
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

//! Derive macros (`FromEntity`, `IntoEntity`, `FromValue`, `IntoValue`) for the Google Cloud Datastore client.
//!
//! # Supported `#[datastore(...)]` Attributes
//!
//! - Container attributes: `#[datastore(rename_all = "camelCase" | "snake_case" | "PascalCase" | "SCREAMING_SNAKE_CASE" | "kebab-case")]`
//! - Field attributes:
//!   - `#[datastore(rename = "property_name")]`
//!   - `#[datastore(exclude_from_indexes)]`
//!   - `#[datastore(key)]` (maps `Option<Key>` or `Key` to `Entity::key()`)
//!   - `#[datastore(default)]` (uses `Default::default()` when the property is missing or null)
//!   - `#[datastore(flatten)]` (inlines nested struct properties into the parent entity)

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenameRule {
    Camel,
    Snake,
    Pascal,
    ScreamingSnake,
    Kebab,
}

impl RenameRule {
    fn from_str(s: &str, span: proc_macro2::Span) -> syn::Result<Self> {
        match s {
            "camelCase" => Ok(Self::Camel),
            "snake_case" => Ok(Self::Snake),
            "PascalCase" => Ok(Self::Pascal),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnake),
            "kebab-case" => Ok(Self::Kebab),
            other => Err(syn::Error::new(
                span,
                format!(
                    "unsupported rename_all rule `{other}`; expected one of: camelCase, snake_case, PascalCase, SCREAMING_SNAKE_CASE, kebab-case"
                ),
            )),
        }
    }

    fn apply(self, ident: &str) -> String {
        let words: Vec<&str> = ident.split('_').filter(|w| !w.is_empty()).collect();
        match self {
            Self::Snake => words
                .iter()
                .map(|w| w.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("_"),
            Self::ScreamingSnake => words
                .iter()
                .map(|w| w.to_ascii_uppercase())
                .collect::<Vec<_>>()
                .join("_"),
            Self::Kebab => words
                .iter()
                .map(|w| w.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("-"),
            Self::Pascal => words
                .iter()
                .map(|w| capitalize(w))
                .collect::<Vec<_>>()
                .join(""),
            Self::Camel => {
                let mut out = String::new();
                for (idx, w) in words.iter().enumerate() {
                    if idx == 0 {
                        out.push_str(&w.to_ascii_lowercase());
                    } else {
                        out.push_str(&capitalize(w));
                    }
                }
                out
            }
        }
    }
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
        }
    }
}

#[derive(Default)]
struct ContainerAttrs {
    rename_all: Option<RenameRule>,
}

fn parse_container_attrs(attrs: &[syn::Attribute]) -> syn::Result<ContainerAttrs> {
    let mut out = ContainerAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("datastore") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                out.rename_all = Some(RenameRule::from_str(&lit.value(), lit.span())?);
                Ok(())
            } else {
                Err(meta.error("unsupported datastore container attribute; expected `rename_all`"))
            }
        })?;
    }
    Ok(out)
}

#[derive(Default)]
struct FieldAttrs {
    rename: Option<String>,
    exclude_from_indexes: bool,
    is_key: bool,
    is_default: bool,
    is_flatten: bool,
}

fn parse_field_attrs(field: &syn::Field) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in &field.attrs {
        if !attr.path().is_ident("datastore") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                out.rename = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("exclude_from_indexes") {
                out.exclude_from_indexes = true;
                Ok(())
            } else if meta.path.is_ident("key") {
                out.is_key = true;
                Ok(())
            } else if meta.path.is_ident("default") {
                out.is_default = true;
                Ok(())
            } else if meta.path.is_ident("flatten") {
                out.is_flatten = true;
                Ok(())
            } else {
                Err(meta.error(
                    "unsupported datastore field attribute; expected `rename`, `exclude_from_indexes`, `key`, `default`, or `flatten`",
                ))
            }
        })?;
    }
    if out.is_key && (out.is_flatten || out.rename.is_some() || out.exclude_from_indexes) {
        return Err(syn::Error::new_spanned(
            field,
            "`#[datastore(key)]` cannot be combined with `rename`, `exclude_from_indexes`, or `flatten`",
        ));
    }
    if out.is_flatten && (out.rename.is_some() || out.exclude_from_indexes || out.is_default) {
        return Err(syn::Error::new_spanned(
            field,
            "`#[datastore(flatten)]` cannot be combined with `rename`, `exclude_from_indexes`, or `default`",
        ));
    }
    Ok(out)
}

fn resolve_property_name(
    field_ident: &syn::Ident,
    container_attrs: &ContainerAttrs,
    field_attrs: &FieldAttrs,
) -> String {
    if let Some(ref renamed) = field_attrs.rename {
        return renamed.clone();
    }
    let raw = field_ident.unraw().to_string();
    if let Some(rule) = container_attrs.rename_all {
        rule.apply(&raw)
    } else {
        raw
    }
}

/// Derives `google_cloud_datastore::entity::FromEntity` and `TryFrom<Entity>` for a named struct.
#[proc_macro_derive(FromEntity, attributes(datastore))]
pub fn derive_from_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_entity_impl(input).into()
}

fn derive_from_entity_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;
    let container_attrs = match parse_container_attrs(&input.attrs) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromEntity can only be derived for structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromEntity can only be derived for structs with named fields",
            )
            .to_compile_error();
        }
    };

    let mut field_inits = Vec::with_capacity(fields.len());
    for field in &fields {
        let field_attrs = match parse_field_attrs(field) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error(),
        };
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        if field_attrs.is_key {
            field_inits.push(quote! {
                #ident: <#ty as ::google_cloud_datastore::entity::EntityKeyField>::from_entity_key(__datastore_entity)?,
            });
        } else if field_attrs.is_flatten {
            field_inits.push(quote! {
                #ident: <#ty as ::google_cloud_datastore::entity::FromEntity>::from_entity(__datastore_entity)?,
            });
        } else {
            let prop_name = resolve_property_name(ident, &container_attrs, &field_attrs);
            if field_attrs.is_default {
                field_inits.push(quote! {
                    #ident: match __datastore_entity.properties().get(#prop_name) {
                        ::core::option::Option::Some(__v) if !__v.is_null() => {
                            <#ty as ::google_cloud_datastore::value::FromValue>::from_value(__v).map_err(|source| {
                                ::google_cloud_datastore::error::EntityError::InvalidProperty {
                                    name: ::std::string::String::from(#prop_name),
                                    source,
                                }
                            })?
                        }
                        _ => ::core::default::Default::default(),
                    },
                });
            } else {
                field_inits.push(quote! {
                    #ident: __datastore_entity.get::<#ty>(#prop_name)?,
                });
            }
        }
    }

    let mut generics = input.generics;
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(
                ::google_cloud_datastore::value::FromValue
            ));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::google_cloud_datastore::entity::FromEntity for #name #ty_generics #where_clause {
            fn from_entity(
                __datastore_entity: &::google_cloud_datastore::entity::Entity,
            ) -> ::core::result::Result<Self, ::google_cloud_datastore::error::EntityError> {
                ::core::result::Result::Ok(Self {
                    #( #field_inits )*
                })
            }
        }

        impl #impl_generics ::core::convert::TryFrom<::google_cloud_datastore::entity::Entity> for #name #ty_generics #where_clause {
            type Error = ::google_cloud_datastore::error::EntityError;

            fn try_from(
                __datastore_entity: ::google_cloud_datastore::entity::Entity,
            ) -> ::core::result::Result<Self, Self::Error> {
                <Self as ::google_cloud_datastore::entity::FromEntity>::from_entity_owned(__datastore_entity)
            }
        }

        impl #impl_generics ::core::convert::TryFrom<&::google_cloud_datastore::entity::Entity> for #name #ty_generics #where_clause {
            type Error = ::google_cloud_datastore::error::EntityError;

            fn try_from(
                __datastore_entity: &::google_cloud_datastore::entity::Entity,
            ) -> ::core::result::Result<Self, Self::Error> {
                <Self as ::google_cloud_datastore::entity::FromEntity>::from_entity(__datastore_entity)
            }
        }
    }
}

/// Derives `google_cloud_datastore::entity::IntoEntity` and `From<T> for Entity` for a named struct.
#[proc_macro_derive(IntoEntity, attributes(datastore))]
pub fn derive_into_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_into_entity_impl(input).into()
}

fn derive_into_entity_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;
    let container_attrs = match parse_container_attrs(&input.attrs) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "IntoEntity can only be derived for structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "IntoEntity can only be derived for structs with named fields",
            )
            .to_compile_error();
        }
    };

    let mut stmts = Vec::with_capacity(fields.len());
    for field in &fields {
        let field_attrs = match parse_field_attrs(field) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error(),
        };
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        if field_attrs.is_key {
            stmts.push(quote! {
                <#ty as ::google_cloud_datastore::entity::EntityKeyField>::apply_to_entity(
                    self.#ident,
                    &mut __datastore_entity,
                );
            });
        } else if field_attrs.is_flatten {
            stmts.push(quote! {
                {
                    let __flattened = <#ty as ::google_cloud_datastore::entity::IntoEntity>::into_entity(self.#ident);
                    for (__k, __v) in __flattened.properties() {
                        __datastore_entity.properties_mut().insert(__k.clone(), __v.clone());
                    }
                }
            });
        } else {
            let prop_name = resolve_property_name(ident, &container_attrs, &field_attrs);
            if field_attrs.exclude_from_indexes {
                stmts.push(quote! {
                    __datastore_entity.set_unindexed(#prop_name, self.#ident);
                });
            } else {
                stmts.push(quote! {
                    __datastore_entity.set(#prop_name, self.#ident);
                });
            }
        }
    }

    let mut generics = input.generics;
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(
                ::google_cloud_datastore::value::IntoValue
            ));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::google_cloud_datastore::entity::IntoEntity for #name #ty_generics #where_clause {
            fn into_entity(self) -> ::google_cloud_datastore::entity::Entity {
                let mut __datastore_entity = ::google_cloud_datastore::entity::Entity::new();
                #( #stmts )*
                __datastore_entity
            }
        }

        impl #impl_generics ::core::convert::From<#name #ty_generics> for ::google_cloud_datastore::entity::Entity #where_clause {
            fn from(value: #name #ty_generics) -> Self {
                <#name #ty_generics as ::google_cloud_datastore::entity::IntoEntity>::into_entity(value)
            }
        }
    }
}

/// Derives `google_cloud_datastore::value::FromValue` for a named struct (stored as an embedded `Entity` value).
#[proc_macro_derive(FromValue, attributes(datastore))]
pub fn derive_from_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_from_value_impl(input).into()
}

fn derive_from_value_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident.clone();
    let container_attrs = match parse_container_attrs(&input.attrs) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "FromValue can only be derived for structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "FromValue can only be derived for structs with named fields",
            )
            .to_compile_error();
        }
    };

    let mut field_inits = Vec::with_capacity(fields.len());
    for field in &fields {
        let field_attrs = match parse_field_attrs(field) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error(),
        };
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        if field_attrs.is_key {
            field_inits.push(quote! {
                #ident: <#ty as ::google_cloud_datastore::entity::EntityKeyField>::from_entity_key(__datastore_entity)
                    .map_err(|e| ::google_cloud_datastore::error::ValueError::Custom(e.to_string()))?,
            });
        } else if field_attrs.is_flatten {
            field_inits.push(quote! {
                #ident: <#ty as ::google_cloud_datastore::entity::FromEntity>::from_entity(__datastore_entity)
                    .map_err(|e| ::google_cloud_datastore::error::ValueError::Custom(e.to_string()))?,
            });
        } else {
            let prop_name = resolve_property_name(ident, &container_attrs, &field_attrs);
            if field_attrs.is_default {
                field_inits.push(quote! {
                    #ident: match __datastore_entity.properties().get(#prop_name) {
                        ::core::option::Option::Some(__v) if !__v.is_null() => {
                            <#ty as ::google_cloud_datastore::value::FromValue>::from_value(__v)?
                        }
                        _ => ::core::default::Default::default(),
                    },
                });
            } else {
                field_inits.push(quote! {
                    #ident: __datastore_entity
                        .get::<#ty>(#prop_name)
                        .map_err(|e| ::google_cloud_datastore::error::ValueError::Custom(e.to_string()))?,
                });
            }
        }
    }

    let mut generics = input.generics;
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(
                ::google_cloud_datastore::value::FromValue
            ));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::google_cloud_datastore::value::FromValue for #name #ty_generics #where_clause {
            fn from_value(
                __datastore_value: &::google_cloud_datastore::value::Value,
            ) -> ::core::result::Result<Self, ::google_cloud_datastore::error::ValueError> {
                match __datastore_value.kind() {
                    ::google_cloud_datastore::value::ValueKind::Entity(__datastore_entity) => {
                        ::core::result::Result::Ok(Self {
                            #( #field_inits )*
                        })
                    }
                    __other => ::core::result::Result::Err(
                        ::google_cloud_datastore::error::ValueError::UnexpectedType {
                            expected: "Entity",
                            actual: __other.type_name(),
                        },
                    ),
                }
            }
        }
    }
}

/// Derives `google_cloud_datastore::value::IntoValue` for a named struct (stored as an embedded `Entity` value).
#[proc_macro_derive(IntoValue, attributes(datastore))]
pub fn derive_into_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_into_value_impl(input).into()
}

fn derive_into_value_impl(input: DeriveInput) -> proc_macro2::TokenStream {
    let name = input.ident;
    let container_attrs = match parse_container_attrs(&input.attrs) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "IntoValue can only be derived for structs with named fields",
                )
                .to_compile_error();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "IntoValue can only be derived for structs with named fields",
            )
            .to_compile_error();
        }
    };

    let mut stmts = Vec::with_capacity(fields.len());
    for field in &fields {
        let field_attrs = match parse_field_attrs(field) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error(),
        };
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        if field_attrs.is_key {
            stmts.push(quote! {
                <#ty as ::google_cloud_datastore::entity::EntityKeyField>::apply_to_entity(
                    self.#ident,
                    &mut __datastore_entity,
                );
            });
        } else if field_attrs.is_flatten {
            stmts.push(quote! {
                {
                    let __flattened = <#ty as ::google_cloud_datastore::entity::IntoEntity>::into_entity(self.#ident);
                    for (__k, __v) in __flattened.properties() {
                        __datastore_entity.properties_mut().insert(__k.clone(), __v.clone());
                    }
                }
            });
        } else {
            let prop_name = resolve_property_name(ident, &container_attrs, &field_attrs);
            if field_attrs.exclude_from_indexes {
                stmts.push(quote! {
                    __datastore_entity.set_unindexed(#prop_name, self.#ident);
                });
            } else {
                stmts.push(quote! {
                    __datastore_entity.set(#prop_name, self.#ident);
                });
            }
        }
    }

    let mut generics = input.generics;
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(
                ::google_cloud_datastore::value::IntoValue
            ));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::google_cloud_datastore::value::IntoValue for #name #ty_generics #where_clause {
            fn into_value(self) -> ::google_cloud_datastore::value::Value {
                let mut __datastore_entity = ::google_cloud_datastore::entity::Entity::new();
                #( #stmts )*
                ::google_cloud_datastore::value::Value::from_kind(
                    ::google_cloud_datastore::value::ValueKind::Entity(__datastore_entity),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn rename_rule_transformations() {
        let span = proc_macro2::Span::call_site();
        assert_eq!(
            RenameRule::from_str("camelCase", span)
                .unwrap()
                .apply("user_display_name"),
            "userDisplayName"
        );
        assert_eq!(
            RenameRule::from_str("PascalCase", span)
                .unwrap()
                .apply("user_display_name"),
            "UserDisplayName"
        );
        assert_eq!(
            RenameRule::from_str("SCREAMING_SNAKE_CASE", span)
                .unwrap()
                .apply("user_display_name"),
            "USER_DISPLAY_NAME"
        );
        assert_eq!(
            RenameRule::from_str("kebab-case", span)
                .unwrap()
                .apply("user_display_name"),
            "user-display-name"
        );
        assert!(RenameRule::from_str("invalidCase", span).is_err());
    }

    #[test]
    fn reject_conflicting_field_attributes() {
        let input: DeriveInput = parse_quote! {
            struct Bad {
                #[datastore(key, rename = "other")]
                id: String,
            }
        };
        let tokens = derive_from_entity_impl(input).to_string();
        assert!(tokens.contains("compile_error"));
    }
}
