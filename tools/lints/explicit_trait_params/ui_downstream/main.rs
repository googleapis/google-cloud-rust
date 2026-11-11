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

// aux-build:trait_defs.rs
#![allow(dead_code, unused_imports)]

// Downstream crate: the traits are defined in `trait_defs` (see
// `auxiliary/trait_defs.rs`).
extern crate trait_defs;

use trait_defs::Foreign;
use trait_defs::prost::{self, FromProto, ToProto};

pub struct Local;
pub struct LocalProto;
pub struct OtherProto;

impl ToProto<LocalProto> for Local {
    type Output = LocalProto;
    fn to_proto(self) -> prost::Result<LocalProto> {
        Ok(LocalProto)
    }
}
impl ToProto<LocalProto> for &Local {
    type Output = LocalProto;
    fn to_proto(self) -> prost::Result<LocalProto> {
        Ok(LocalProto)
    }
}
// Allowed by the orphan rules: `Box` is #[fundamental], and for `Option<_>`
// the trait parameter `LocalProto` is local.
impl ToProto<LocalProto> for Box<Local> {
    type Output = LocalProto;
    fn to_proto(self) -> prost::Result<LocalProto> {
        Ok(LocalProto)
    }
}
impl ToProto<LocalProto> for Option<Local> {
    type Output = LocalProto;
    fn to_proto(self) -> prost::Result<LocalProto> {
        Ok(LocalProto)
    }
}
// A downstream impl for a foreign type (allowed: `OtherProto` is local).
impl ToProto<OtherProto> for Foreign {
    type Output = OtherProto;
    fn to_proto(self) -> prost::Result<OtherProto> {
        Ok(OtherProto)
    }
}

fn local_receiver(l: Local) {
    let _ = Local.to_proto(); // OK: local
    let _ = ToProto::to_proto(Local); // OK: local
    let _ = Local::to_proto(Local); // OK: local
    let _ = (&l).to_proto(); // OK: `&Local` peels to local
}

fn foreign_receiver(s: String) {
    let _ = s.clone().to_proto(); // FLAG: String
    let _ = 1i32.to_proto(); // FLAG: i32
    let _: prost::Result<String> = s.clone().cnv(); // FLAG: String
    let _ = ToProto::to_proto(s.clone()); // FLAG
    let _ = String::to_proto(s.clone()); // FLAG
    let _: Vec<prost::Result<String>> = vec![s].into_iter().map(ToProto::to_proto).collect(); // FLAG
}

fn foreign_adt_with_two_impls() {
    // `Foreign.to_proto()` would be E0283 here (impls for ForeignProto and
    // OtherProto); the qualified forms are required and are OK.
    let _ = ToProto::<trait_defs::ForeignProto>::to_proto(Foreign); // OK
    let _ = <Foreign as ToProto<OtherProto>>::to_proto(Foreign); // OK
}

fn qualified(s: String) {
    let _ = ToProto::<String>::to_proto(s.clone()); // OK
    let _ = prost::ToProto::<String>::to_proto(s.clone()); // OK
    let _ = trait_defs::prost::FromProto::<String>::cnv(s.clone()); // OK
    let _ = <String as ToProto<String>>::to_proto(s.clone()); // OK
    let _ = <String as ToProto<_>>::to_proto(s.clone()); // FLAG: `_` is not explicit
    let _: Vec<_> = vec![s]
        .into_iter()
        .map(ToProto::<String>::to_proto)
        .collect(); // OK
}

fn generic<T: ToProto<String>>(t: T) {
    let _ = t.to_proto(); // OK: resolved through the bound
}

fn generic_ref<T>(t: &T)
where
    for<'a> &'a T: ToProto<String>,
{
    let _ = t.to_proto(); // OK: `&T` peels to a type param
}

fn projection<I: Iterator>(mut i: I)
where
    I::Item: ToProto<String>,
{
    let _ = i.next().map(|x| x.to_proto()); // OK: projection
}

fn wrappers(b: Box<Local>, o: Option<Local>) {
    let _ = b.to_proto(); // FLAG: the trait's crate could add `impl<T> ToProto<_> for Box<T>`
    let _ = o.to_proto(); // FLAG: same for `Option<T>`
}

fn main() {}
