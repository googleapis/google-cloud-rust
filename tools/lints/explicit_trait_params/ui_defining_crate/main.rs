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

// The traits are defined in this crate, so no call here is flagged: calls
// inside the defining crate cannot break from version skew. "FLAG" comments
// mark the unqualified forms that would be flagged in a downstream crate.
// The only expected warnings are for the blanket impls in `mod blanket`.

#![allow(dead_code, unused_imports)]

// The traits live in this crate, so this also covers the "defining crate"
// case (the call sites reach the trait via `crate::prost::ToProto`).
pub mod prost {
    pub type Result<T> = std::result::Result<T, ()>;

    pub trait ToProto<T>: Sized {
        type Output;
        fn to_proto(self) -> Result<Self::Output>;
    }

    pub trait FromProto<T>: Sized {
        fn cnv(self) -> Result<T>;
    }

    // Trait impls are not calls: OK.
    impl ToProto<String> for String {
        type Output = String;
        fn to_proto(self) -> Result<String> {
            Ok(self)
        }
    }
    impl FromProto<String> for String {
        fn cnv(self) -> Result<String> {
            Ok(self)
        }
    }

    pub fn inside_defining_module(s: String) {
        let _ = s.clone().to_proto(); // FLAG
        let _ = ToProto::<String>::to_proto(s.clone()); // OK
        let _ = crate::prost::ToProto::<String>::to_proto(s); // OK
    }
}

use prost::{FromProto, ToProto};

pub struct Model;
pub struct ProtoA;
pub struct ProtoB;

// Two conflicting impls for the same Self type.
impl ToProto<ProtoA> for Model {
    type Output = ProtoA;
    fn to_proto(self) -> prost::Result<ProtoA> {
        Ok(ProtoA)
    }
}
impl ToProto<ProtoB> for Model {
    type Output = ProtoB;
    fn to_proto(self) -> prost::Result<ProtoB> {
        Ok(ProtoB)
    }
}

fn method_syntax(s: String) {
    let _ = s.clone().to_proto(); // FLAG
    let _: prost::Result<String> = s.cnv(); // FLAG
}

fn unqualified_paths(s: String) {
    let _ = ToProto::to_proto(s.clone()); // FLAG
    let _: prost::Result<String> = FromProto::cnv(s.clone()); // FLAG
    let _ = prost::ToProto::to_proto(s.clone()); // FLAG
    let _ = crate::prost::ToProto::to_proto(s.clone()); // FLAG
    let _ = String::to_proto(s.clone()); // FLAG (type-relative)
    let _ = <String as ToProto<_>>::to_proto(s.clone()); // FLAG (`_` is inferred)
    let _: Vec<prost::Result<String>> =
        vec![s.clone()].into_iter().map(ToProto::to_proto).collect(); // FLAG
    let _: Vec<prost::Result<String>> = vec![s].into_iter().map(FromProto::cnv).collect(); // FLAG
}

fn qualified_paths(s: String) {
    let _ = ToProto::<String>::to_proto(s.clone()); // OK
    let _ = FromProto::<String>::cnv(s.clone()); // OK
    let _ = prost::ToProto::<String>::to_proto(s.clone()); // OK
    let _ = crate::prost::ToProto::<String>::to_proto(s.clone()); // OK
    let _ = <String as ToProto<String>>::to_proto(s.clone()); // OK
    let _: Vec<_> = vec![s.clone()]
        .into_iter()
        .map(ToProto::<String>::to_proto)
        .collect(); // OK
    let _: Vec<_> = vec![s].into_iter().map(FromProto::<String>::cnv).collect(); // OK
}

fn conflicting_impls() {
    // `Model.to_proto()` would be E0283 here; the qualified forms work.
    let _ = ToProto::<ProtoA>::to_proto(Model); // OK
    let _ = ToProto::<ProtoB>::to_proto(Model); // OK
    let _ = <Model as ToProto<ProtoA>>::to_proto(Model); // OK
    let _: prost::Result<ProtoA> = <Model as ToProto<ProtoA>>::to_proto(Model); // OK
}

fn generic<T: ToProto<String>>(t: T) {
    let _ = t.to_proto(); // FLAG
}

macro_rules! local_macro {
    ($e:expr) => {
        $e.to_proto()
    };
}

fn in_local_macro(s: String) {
    let _ = local_macro!(s); // FLAG (local macro, not external)
}

// Impl check. A separate trait, so the blanket impls do not make the calls
// above ambiguous.
pub mod blanket {
    pub trait ToProto<T>: Sized {
        fn to_proto(self) -> T;
    }

    pub struct ProtoA;
    pub struct ProtoB;
    pub struct ProtoC;
    pub struct ProtoD;
    pub struct ProtoE;
    pub struct ProtoF;
    pub struct Model;

    // Blanket impls over a bare type parameter: FLAG.
    impl<T> ToProto<ProtoA> for T {
        fn to_proto(self) -> ProtoA {
            ProtoA
        }
    }
    impl<'a, T> ToProto<ProtoB> for &'a T {
        fn to_proto(self) -> ProtoB {
            ProtoB
        }
    }

    // An intentional blanket impl, allowed: OK.
    #[allow(explicit_trait_params)]
    impl<T: Clone> ToProto<ProtoC> for T {
        fn to_proto(self) -> ProtoC {
            ProtoC
        }
    }

    // Wrapper impls: OK (calls on wrapper receivers are flagged instead).
    impl<T> ToProto<ProtoD> for Option<T> {
        fn to_proto(self) -> ProtoD {
            ProtoD
        }
    }
    impl<T> ToProto<ProtoD> for Vec<T> {
        fn to_proto(self) -> ProtoD {
            ProtoD
        }
    }

    // Concrete impls: OK.
    impl ToProto<ProtoE> for String {
        fn to_proto(self) -> ProtoE {
            ProtoE
        }
    }
    impl ToProto<ProtoF> for Model {
        fn to_proto(self) -> ProtoF {
            ProtoF
        }
    }
}

fn main() {}
