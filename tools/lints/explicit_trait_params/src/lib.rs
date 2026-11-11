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

#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_data_structures;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_middle;
extern crate rustc_span;

use rustc_data_structures::fx::FxHashMap;
use rustc_errors::Applicability;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::DefId;
use rustc_hir::{
    self as hir, Expr, ExprKind, GenericArg, HirId, Item, ItemKind, Path, PathSegment, QPath,
    TyKind,
};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_middle::ty::{self, GenericArgsRef, Ty};
use rustc_span::{Span, Symbol};
use serde::Deserialize;

dylint_linting::impl_late_lint! {
    /// ### What it does
    ///
    /// Flags calls to methods of the configured traits that do not name the
    /// trait's generic parameters (`x.to_proto()`, `ToProto::to_proto(x)`,
    /// `String::to_proto`, `<X as ToProto<_>>::to_proto`), but only when the
    /// call can break from version skew: the call is outside the trait's
    /// defining crate, and the receiver type (after peeling references) is
    /// neither a generic parameter or projection nor a type defined in the
    /// calling crate. See `why_flag` for the full reasoning.
    ///
    /// It also flags blanket impls of the configured traits
    /// (`impl<T> ToProto<X> for T`, including `for &T`). Such an impl adds a
    /// candidate for every receiver type, so it can break unqualified calls on
    /// any type, and cargo-semver-checks does not report it.
    ///
    /// ### Why is this bad?
    ///
    /// The call compiles because exactly one impl applies to the receiver type.
    /// A semver-compatible release of another crate can add a second impl for
    /// that type, which makes the call ambiguous (E0283) and breaks callers
    /// that are already published.
    ///
    /// ### Configuration
    ///
    /// List traits that cannot be sealed (other crates must implement them)
    /// and that take a trait-level generic parameter, so a single type can
    /// have several impls spread across independently released crates. Sealed
    /// traits keep all impls in one crate and do not need this lint.
    ///
    /// ```toml
    /// [explicit_trait_params]
    /// traits = ["google_cloud_gax_internal::prost::ToProto"]
    /// ```
    ///
    /// ### Example
    ///
    /// ```rust,ignore
    /// // `message: String`
    /// let m = self.message.to_proto()?;
    /// ```
    ///
    /// Use instead:
    ///
    /// ```rust,ignore
    /// let m = ToProto::<String>::to_proto(self.message)?;
    /// ```
    pub EXPLICIT_TRAIT_PARAMS,
    Warn,
    "calls to methods of configured traits must specify the trait's generic parameters",
    ExplicitTraitParams::new()
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Fully qualified trait paths, starting with the crate name (e.g.
    /// `google_cloud_gax_internal::prost::ToProto`).
    #[serde(default)]
    traits: Vec<String>,
}

struct ExplicitTraitParams {
    config: Config,
    /// Cache: trait DefId -> is it one of the configured traits?
    cache: FxHashMap<DefId, bool>,
}

impl ExplicitTraitParams {
    fn new() -> Self {
        Self {
            config: dylint_linting::config_or_default(env!("CARGO_PKG_NAME")),
            cache: FxHashMap::default(),
        }
    }

    fn is_configured(&mut self, cx: &LateContext<'_>, trait_did: DefId) -> bool {
        let traits = &self.config.traits;
        *self.cache.entry(trait_did).or_insert_with(|| {
            // Compare by def path (crate name + path within the crate), not by
            // the textual path at the call site. This works for downstream
            // crates as well as for the defining crate (`crate::prost::ToProto`).
            let path = format!(
                "{}{}",
                cx.tcx.crate_name(trait_did.krate),
                cx.tcx.def_path(trait_did).to_string_no_crate_verbose()
            );
            traits.contains(&path)
        })
    }

    /// Returns the trait DefId if `method_did` is a method of a configured trait.
    fn configured_trait_of(&mut self, cx: &LateContext<'_>, method_did: DefId) -> Option<DefId> {
        let trait_did = cx.tcx.trait_of_assoc(method_did)?;
        self.is_configured(cx, trait_did).then_some(trait_did)
    }

    /// Decides whether a call with this `Self` type can break from version
    /// skew. Returns `None` to skip the call, or `Some(reason)` to flag it.
    ///
    /// The hazard: the call compiles today because exactly one
    /// `impl Trait<P> for Self` applies. A semver-compatible release of some
    /// *other* crate adds a second `impl Trait<Q> for Self`, and the call
    /// becomes ambiguous (E0283). We only flag calls where such an impl can
    /// legally appear outside the calling crate:
    ///
    /// (a) Calls inside the crate that defines the trait are never flagged: a
    ///     conflicting impl added there breaks that crate's own build/CI, not a
    ///     downstream user.
    /// (b) `Self` is a type parameter or projection (`T`, `<T as X>::Y`): the
    ///     method resolves through the where-clause bounds in scope, not by
    ///     impl selection, so new impls cannot make it ambiguous.
    /// (c) `Self` (after peeling `&`/`&mut`) is an ADT defined in the calling
    ///     crate: a dependency cannot name `Local` (that would need a circular
    ///     dependency), so it cannot add a concrete `impl Trait<_> for Local`.
    ///     Crates that depend on the calling crate can add such impls, but
    ///     they are not visible when the calling crate compiles. The trait's
    ///     crate could still add a blanket `impl<T> Trait<X> for T`, which adds
    ///     a candidate for every receiver; that case is covered by the impl
    ///     check (`check_impl`), which runs over the trait's crate.
    ///
    /// Everything else is flagged, conservatively:
    /// - Foreign ADTs and primitives (`String`, `i32`, `wkt::Timestamp`,
    ///   `prost_types::*`): the trait's crate can add impls directly.
    /// - Wrappers of local types (`Box<Local>`, `Option<Local>`,
    ///   `Vec<Local>`, `[Local]`, tuples, arrays): the trait's crate can add
    ///   blanket impls such as `impl<T: Trait<P>> Trait<Option<P>> for
    ///   Option<T>`, which would then compete with any impl this crate has for
    ///   `Option<Local>`. `Box` is `#[fundamental]`, so `Box<Local>` is "local"
    ///   for orphan purposes, but the trait's crate can still write
    ///   `impl<T> Trait<X> for Box<T>`; flag it too.
    /// - Anything else (`dyn Trait`, closures, fn pointers, ...).
    fn why_flag<'tcx>(
        &self,
        cx: &LateContext<'tcx>,
        trait_did: DefId,
        self_ty: Ty<'tcx>,
    ) -> Option<String> {
        // (a)
        if trait_did.is_local() {
            return None;
        }
        let peeled = self_ty.peel_refs();
        match peeled.kind() {
            // (b)
            ty::Param(_) | ty::Alias(..) => None,
            // (c)
            ty::Adt(adt, _) if adt.did().is_local() => None,
            _ => {
                let trait_name = cx.tcx.item_name(trait_did);
                let ty = ty::print::with_forced_trimmed_paths!(peeled.to_string());
                Some(format!(
                    "receiver type `{ty}` is not defined in this crate; a future `{trait_name}` impl in another crate could make this call ambiguous"
                ))
            }
        }
    }
}

/// Renders the trait's own generic args (excluding `Self`), e.g. `<P>`.
fn trait_args_str<'tcx>(
    cx: &LateContext<'tcx>,
    trait_did: DefId,
    args: GenericArgsRef<'tcx>,
) -> String {
    // The trait ref's args are [Self, T1, T2, ...]; skip `Self`.
    let trait_ref = ty::TraitRef::from_assoc(cx.tcx, trait_did, args);
    let own: Vec<String> = trait_ref
        .args
        .iter()
        .skip(1)
        .map(|a| ty::print::with_forced_trimmed_paths!(a.to_string()))
        .collect();
    if own.is_empty() {
        String::new()
    } else {
        format!("<{}>", own.join(", "))
    }
}

/// Whether the user wrote concrete (non-`_`) generic args on the segment.
fn has_explicit_args(seg: &PathSegment<'_>) -> bool {
    seg.args.is_some_and(|args| {
        !args.args.is_empty()
            && !args.args.iter().any(|a| match a {
                GenericArg::Infer(_) => true,
                GenericArg::Type(t) => matches!(t.kind, TyKind::Infer(_)),
                _ => false,
            })
    })
}

/// What the per-form checks need to report a call.
struct Finding {
    trait_did: DefId,
    trait_name: Symbol,
    /// The trait's own generic args as written in the suggestion, e.g. `<P>`.
    trait_args: String,
    /// Why this call can break from version skew (see `why_flag`).
    reason: String,
}

impl ExplicitTraitParams {
    /// Returns a `Finding` if the expression `hir_id` calls (or names)
    /// `method_did`, a method of a configured trait, in a way that can break
    /// from version skew.
    fn finding<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        hir_id: HirId,
        method_did: DefId,
    ) -> Option<Finding> {
        let trait_did = self.configured_trait_of(cx, method_did)?;
        let args = cx.typeck_results().node_args(hir_id);
        let reason = self.why_flag(cx, trait_did, args.type_at(0))?;
        Some(Finding {
            trait_did,
            trait_name: cx.tcx.item_name(trait_did),
            trait_args: trait_args_str(cx, trait_did, args),
            reason,
        })
    }

    /// `x.to_proto()`: method-call syntax cannot specify trait parameters.
    fn check_method_call<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        expr: &'tcx Expr<'tcx>,
        seg: &'tcx PathSegment<'tcx>,
        receiver: &'tcx Expr<'tcx>,
    ) {
        let Some(method_did) = cx.typeck_results().type_dependent_def_id(expr.hir_id) else {
            return;
        };
        let Some(f) = self.finding(cx, expr.hir_id, method_did) else {
            return;
        };
        let (trait_name, targs) = (f.trait_name, &f.trait_args);
        let recv = snippet(cx, receiver.span);
        let sugg = format!("{trait_name}::{targs}::{}({recv})", seg.ident);
        emit(
            cx,
            expr.span,
            format!("method-call syntax cannot specify the generic parameters of `{trait_name}`"),
            f.reason,
            sugg,
        );
    }

    /// `ToProto::to_proto`, `gaxi::prost::ToProto::to_proto`,
    /// `<X as ToProto<_>>::to_proto`: flag unless the trait segment has
    /// explicit (non-`_`) generic args.
    fn check_trait_path<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        expr: &'tcx Expr<'tcx>,
        method_did: DefId,
        path: &'tcx Path<'tcx>,
    ) {
        let Some(f) = self.finding(cx, expr.hir_id, method_did) else {
            return;
        };
        // The trait segment is the one that resolves to the trait.
        let Some(trait_seg) = path
            .segments
            .iter()
            .find(|s| matches!(s.res, Res::Def(DefKind::Trait, d) if d == f.trait_did))
        else {
            return;
        };
        if has_explicit_args(trait_seg) {
            return;
        }
        let span = trait_seg
            .ident
            .span
            .to(trait_seg.args.map_or(trait_seg.ident.span, |a| a.span_ext));
        let sugg = format!("{}::{}", trait_seg.ident, f.trait_args);
        emit(
            cx,
            span,
            format!("generic parameters of `{}` are not specified", f.trait_name),
            f.reason,
            sugg,
        );
    }

    /// `String::to_proto` / `Self::to_proto`: a type-relative path cannot
    /// specify trait parameters.
    fn check_type_relative_path<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        expr: &'tcx Expr<'tcx>,
        method_did: DefId,
        qpath: &'tcx QPath<'tcx>,
        qself: &'tcx hir::Ty<'tcx>,
        seg: &'tcx PathSegment<'tcx>,
    ) {
        let Some(f) = self.finding(cx, expr.hir_id, method_did) else {
            return;
        };
        let (trait_name, targs) = (f.trait_name, &f.trait_args);
        let ty = snippet(cx, qself.span);
        let sugg = format!("<{ty} as {trait_name}{targs}>::{}", seg.ident);
        emit(
            cx,
            qpath.span(),
            format!("generic parameters of `{trait_name}` are not specified"),
            f.reason,
            sugg,
        );
    }
}

impl ExplicitTraitParams {
    /// `impl<T> ToProto<X> for T` (or `for &T`): flag blanket impls of a
    /// configured trait. Such an impl adds a candidate for every receiver
    /// type, including types local to downstream crates, so unqualified
    /// downstream calls that compiled before can become ambiguous. Only the
    /// trait's defining crate can write it (orphan rules), and
    /// cargo-semver-checks does not report it as a breaking change.
    ///
    /// Wrapper impls (`impl<T> ToProto<X> for Option<T>`, `Box<T>`, ...) are
    /// not flagged: they only affect wrapper receivers, which the call checks
    /// already flag.
    fn check_impl<'tcx>(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        let ItemKind::Impl(imp) = item.kind else {
            return;
        };
        if imp.of_trait.is_none() {
            return;
        }
        let trait_did = cx.tcx.impl_trait_id(item.owner_id);
        if !self.is_configured(cx, trait_did) {
            return;
        }
        let self_ty = cx
            .tcx
            .type_of(item.owner_id)
            .instantiate_identity()
            .skip_normalization();
        if !matches!(self_ty.peel_refs().kind(), ty::Param(_)) {
            return;
        }
        let trait_name = cx.tcx.item_name(trait_did);
        cx.emit_span_lint(
            EXPLICIT_TRAIT_PARAMS,
            item.span.with_hi(imp.self_ty.span.hi()),
            rustc_errors::DiagDecorator(move |diag: &mut rustc_errors::Diag<'_, ()>| {
                diag.primary_message(format!(
                    "blanket impl of `{trait_name}` can make unqualified downstream calls ambiguous"
                ));
                diag.note(
                    "adding this is a breaking change for callers that rely on inference (not detected by cargo-semver-checks); if intentional, use `#[allow(explicit_trait_params)]` with a justification and release as a breaking version",
                );
            }),
        );
    }
}

impl<'tcx> LateLintPass<'tcx> for ExplicitTraitParams {
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        if self.config.traits.is_empty() || item.span.in_external_macro(cx.sess().source_map()) {
            return;
        }
        self.check_impl(cx, item);
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if self.config.traits.is_empty() || expr.span.in_external_macro(cx.sess().source_map()) {
            return;
        }
        match expr.kind {
            ExprKind::MethodCall(seg, receiver, _, _) => {
                self.check_method_call(cx, expr, seg, receiver);
            }
            ExprKind::Path(ref qpath) => {
                let Res::Def(DefKind::AssocFn, method_did) = cx.qpath_res(qpath, expr.hir_id)
                else {
                    return;
                };
                match qpath {
                    QPath::Resolved(_, path) => self.check_trait_path(cx, expr, method_did, path),
                    QPath::TypeRelative(qself, seg) => {
                        self.check_type_relative_path(cx, expr, method_did, qpath, qself, seg);
                    }
                }
            }
            _ => {}
        }
    }
}

/// The source text for `span`, or `..` if it is not available.
fn snippet(cx: &LateContext<'_>, span: Span) -> String {
    cx.sess()
        .source_map()
        .span_to_snippet(span)
        .unwrap_or_else(|_| "..".into())
}

fn emit(cx: &LateContext<'_>, span: Span, msg: String, why: String, sugg: String) {
    cx.emit_span_lint(
        EXPLICIT_TRAIT_PARAMS,
        span,
        rustc_errors::DiagDecorator(move |diag: &mut rustc_errors::Diag<'_, ()>| {
            diag.primary_message(msg);
            diag.note(why);
            diag.span_suggestion(
                span,
                "specify the trait parameters",
                sugg,
                Applicability::MaybeIncorrect,
            );
        }),
    );
}

/// Single-crate test: the traits are defined in the test crate itself, so no
/// call is flagged (rule (a)); only the blanket impls in `mod blanket` are.
#[test]
fn ui_defining_crate() {
    dylint_testing::ui::Test::src_base(env!("CARGO_PKG_NAME"), "ui_defining_crate")
        .dylint_toml(
            r#"
[explicit_trait_params]
traits = ["main::prost::ToProto", "main::prost::FromProto", "main::blanket::ToProto"]
"#,
        )
        .run();
}

/// Multi-crate test: the traits live in `ui_downstream/auxiliary/trait_defs.rs`
/// (compiletest `aux-build`), and the test file is a downstream crate.
#[test]
fn ui_downstream() {
    dylint_testing::ui::Test::src_base(env!("CARGO_PKG_NAME"), "ui_downstream")
        .dylint_toml(
            r#"
[explicit_trait_params]
traits = ["trait_defs::prost::ToProto", "trait_defs::prost::FromProto"]
"#,
        )
        .run();
}
