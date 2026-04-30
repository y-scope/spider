//! Implementation of the `#[task]` attribute macro.
//!
//! The macro replaces the annotated function with three items:
//!
//! 1. A unit marker struct that shares the function's identifier and visibility.
//! 2. A private params struct holding the non-context parameters, with
//!    `#[derive(serde::Deserialize)]` so the runtime can rebuild it from wire bytes.
//! 3. An `impl spider_tdl::Task` for the marker struct that wires the params back into the
//!    user-authored function body.
//!
//! See the crate-level docs of [`spider_tdl_derive`] for usage examples.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg,
    GenericArgument,
    ItemFn,
    LitStr,
    Pat,
    PathArguments,
    ReturnType,
    Token,
    Type,
    parse::{Parse, ParseStream},
};

/// Parsed representation of the `#[task(...)]` attribute arguments.
pub struct TaskAttr {
    /// The name alias of the task.
    name: Option<LitStr>,
}

impl Parse for TaskAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self { name: None });
        }

        let ident: syn::Ident = input.parse()?;
        if ident != "name" {
            return Err(syn::Error::new_spanned(&ident, "expected `name = \"...\"`"));
        }
        input.parse::<Token![=]>()?;
        let name: LitStr = input.parse()?;
        Ok(Self { name: Some(name) })
    }
}

/// Expands a `#[task]`-annotated function into a marker struct, params struct, and
/// [`spider_tdl::Task`] trait implementation.
///
/// # NOTE
///
/// * The first parameter's type is *not* validated syntactically here; the generated code includes
///   a private assertion method whose signature requires the type to resolve to
///   [`spider_tdl::TaskContext`], so any mismatch (including aliases that point at an unrelated
///   type) surfaces as a type-checker error at compile time.
/// * The error return type is *not* validated syntactically here; [`build_task_body_wrapper`]
///   syntheses the wrapper with `Result<_, ::spider_tdl::TdlError>` as its return type, so any
///   user-declared `Err` type that does not resolve to `TdlError` produces a type-checker error at
///   compile time.
///
/// # Returns
///
/// The generated token stream on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`validate_no_self`]'s return values on failure.
/// * Forwards [`validate_has_parameters`]'s return values on failure.
/// * Forwards [`extract_return_type`]'s return values on failure.
pub fn expand(attr: &TaskAttr, func: &ItemFn) -> syn::Result<TokenStream> {
    validate_no_self(func)?;
    validate_has_parameters(func)?;

    let func_name = &func.sig.ident;
    let private_task_body_wrapper_name = format_ident!("__{func_name}");
    let params_struct_name = format_ident!("__{func_name}_params");

    let Some(FnArg::Typed(first_pat_type)) = func.sig.inputs.first() else {
        unreachable!("`self` parameters are rejected by `validate_no_self`");
    };
    let first_param_type = &first_pat_type.ty;

    let task_params: Vec<_> = func.sig.inputs.iter().skip(1).collect();
    let param_fields: Vec<TokenStream> = task_params
        .iter()
        .map(|arg| {
            let FnArg::Typed(pat_type) = arg else {
                unreachable!("`self` parameters are rejected by validation");
            };
            let pat = &pat_type.pat;
            let ty = &pat_type.ty;
            quote! { #pat: #ty }
        })
        .collect();
    let param_field_names: Vec<&Pat> = task_params
        .iter()
        .map(|arg| {
            let FnArg::Typed(pat_type) = arg else {
                unreachable!("`self` parameters are rejected by validation");
            };
            pat_type.pat.as_ref()
        })
        .collect();
    let execute_call = if param_field_names.is_empty() {
        quote! { Self::#private_task_body_wrapper_name(ctx) }
    } else {
        let positional_field_accesses =
            param_field_names.iter().map(|name| quote! { params.#name });
        quote! { Self::#private_task_body_wrapper_name(ctx, #(#positional_field_accesses),*) }
    };

    let params_arg = if param_field_names.is_empty() {
        quote! { _params }
    } else {
        quote! { params }
    };

    let (return_type_tokens, needs_return_wrapping) = extract_return_type(&func.sig.output)?;
    let private_task_body_wrapper = build_task_body_wrapper(
        &private_task_body_wrapper_name,
        func,
        &return_type_tokens,
        needs_return_wrapping,
    );

    let task_name_str = attr
        .name
        .as_ref()
        .map_or_else(|| func_name.to_string(), LitStr::value);

    let vis = &func.vis;

    let expanded = quote! {
        #[allow(non_camel_case_types)]
        #vis struct #func_name;

        impl #func_name {
            // A compile-time assertion that the parameter type resolves to
            // `::spider_tdl::TaskContext`. Returning `ctx` forces the compiler to require type
            // equality (a transparent alias counts; an unrelated newtype does not). The function is
            // never invoked at runtime.
            #[allow(dead_code, non_snake_case, clippy::needless_pass_by_value)]
            fn __assert_first_param_is_task_context(
                ctx: #first_param_type,
            ) -> ::spider_tdl::TaskContext {
                ctx
            }

            #private_task_body_wrapper
        }

        #[allow(non_camel_case_types)]
        #[derive(::serde::Deserialize)]
        struct #params_struct_name {
            #(#param_fields,)*
        }

        impl ::spider_tdl::Task for #func_name {
            type Params = #params_struct_name;
            type Return = #return_type_tokens;

            const NAME: &'static str = #task_name_str;

            fn execute(
                ctx: ::spider_tdl::TaskContext,
                #params_arg: Self::Params,
            ) -> ::std::result::Result<Self::Return, ::spider_tdl::TdlError> {
                #execute_call
            }
        }
    };

    Ok(expanded)
}

/// Builds the private wrapper that holds the original user task body.
///
/// # Returns
///
/// The token stream of the built private method.
fn build_task_body_wrapper(
    task_body_wrapper_name: &syn::Ident,
    func: &ItemFn,
    return_type_tokens: &TokenStream,
    needs_return_wrapping: bool,
) -> TokenStream {
    let original_params = &func.sig.inputs;
    let original_body = &func.block;

    if needs_return_wrapping {
        let original_output = &func.sig.output;
        let ReturnType::Type(_, original_return_type) = original_output else {
            unreachable!("validated that function has a return type");
        };
        let wrapped_return = quote! {
            ::std::result::Result<#return_type_tokens, ::spider_tdl::TdlError>
        };
        quote! {
            #[allow(clippy::redundant_closure_call, clippy::unnecessary_wraps)]
            fn #task_body_wrapper_name(#original_params) -> #wrapped_return {
                (|| -> #original_return_type #original_body)().map(|__v| (__v,))
            }
        }
    } else {
        let standardized_return = quote! {
            ::std::result::Result<#return_type_tokens, ::spider_tdl::TdlError>
        };
        quote! {
            #[allow(clippy::unnecessary_wraps)]
            fn #task_body_wrapper_name(#original_params) -> #standardized_return
                #original_body
        }
    }
}

/// Validates that no parameter is a `self` receiver.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`syn::Error`] if any parameter is a `self` receiver.
fn validate_no_self(func: &ItemFn) -> syn::Result<()> {
    for arg in &func.sig.inputs {
        if let FnArg::Receiver(receiver) = arg {
            return Err(syn::Error::new_spanned(
                receiver,
                "task functions must not have a `self` parameter",
            ));
        }
    }
    Ok(())
}

/// Validates that the function declares at least one parameter (the `TaskContext`).
///
/// # Errors
///
/// Returns an error if:
///
/// * [`syn::Error`] if the function doesn't have parameters.
fn validate_has_parameters(func: &ItemFn) -> syn::Result<()> {
    if func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "task functions must have at least one parameter (`TaskContext`)",
        ));
    }
    Ok(())
}

/// Extracts the `Ok` type of a `Result<...>` return type as a tuple.
///
/// If the user wrote a tuple type (e.g., `Result<(int32, int32), TdlError>`), it is returned
/// as-is. If the user wrote a bare type (e.g., `Result<int32, TdlError>`), it is wrapped into a
/// single-element tuple (`(int32,)`).
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The Ok type in the original result.
/// * A boolean indicating whether the Ok type is a bare type.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`syn::Error`] if:
///   * The function has no return type.
///   * The return type is not a path expression (e.g., a tuple or reference).
///   * The return type's last path segment is not `Result`.
///   * `Result` is missing its generic argument list.
///   * `Result`'s generic argument list size is not 2 (Ok and Err).
///   * `Result`'s first generic argument is not a type.
fn extract_return_type(output: &ReturnType) -> syn::Result<(TokenStream, bool)> {
    const INVALID_RETURN_TYPE_ERROR_MSG: &str =
        "task functions must return `Result<(T, ...), TdlError>`";

    let ReturnType::Type(_, return_type) = output else {
        return Err(syn::Error::new_spanned(
            output,
            INVALID_RETURN_TYPE_ERROR_MSG,
        ));
    };

    let Type::Path(type_path) = return_type.as_ref() else {
        return Err(syn::Error::new_spanned(
            return_type,
            INVALID_RETURN_TYPE_ERROR_MSG,
        ));
    };

    let last_segment = type_path
        .path
        .segments
        .last()
        .expect("return type path should have at least one segment");

    if last_segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            return_type,
            INVALID_RETURN_TYPE_ERROR_MSG,
        ));
    }

    let PathArguments::AngleBracketed(angle_args) = &last_segment.arguments else {
        return Err(syn::Error::new_spanned(
            &last_segment.arguments,
            "expected generic arguments on `Result`",
        ));
    };

    if angle_args.args.len() != 2 {
        return Err(syn::Error::new_spanned(
            angle_args,
            "expected exactly two generic arguments on `Result`",
        ));
    }

    let Some(GenericArgument::Type(ok_type)) = angle_args.args.first() else {
        return Err(syn::Error::new_spanned(
            angle_args,
            "expected a type as the first generic argument of `Result`",
        ));
    };

    if let Type::Tuple(tuple_type) = ok_type {
        Ok((quote! { #tuple_type }, false))
    } else {
        Ok((quote! { (#ok_type,) }, true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `attr_str` as task attributes, `func_str` as a function, expands them, and returns
    /// the normalized token-stream string.
    ///
    /// # Returns
    ///
    /// The normalized token-stream of the expanded function.
    fn expand_to_string(attr_str: &str, func_str: &str) -> String {
        let attr: TaskAttr = syn::parse_str(attr_str).expect("failed to parse task attribute");
        let func: ItemFn = syn::parse_str(func_str).expect("failed to parse function");
        let expanded = expand(&attr, &func).expect("macro expansion failed");
        expanded.to_string()
    }

    /// # Returns
    ///
    /// The normalized token-stream as a string.
    fn normalize(tokens: &TokenStream) -> String {
        tokens.to_string()
    }

    #[test]
    fn expand_task_with_tuple_return() {
        let actual = expand_to_string(
            "",
            r"
            pub(crate) fn add(
                ctx: TaskContext,
                a: int32,
                b: int32,
            ) -> Result<(int32, int32), TdlError> {
                Ok((a + b, a - b))
            }
            ",
        );

        let expected = normalize(&quote! {
            #[allow(non_camel_case_types)]
            pub(crate) struct add;

            impl add {
                #[allow(dead_code, non_snake_case, clippy::needless_pass_by_value)]
                fn __assert_first_param_is_task_context(
                    ctx: TaskContext,
                ) -> ::spider_tdl::TaskContext {
                    ctx
                }

                #[allow(clippy::unnecessary_wraps)]
                fn __add(
                    ctx: TaskContext,
                    a: int32,
                    b: int32,
                ) ->::std::result::Result<(int32, int32), ::spider_tdl::TdlError> {
                    Ok((a + b, a - b))
                }
            }

            #[allow(non_camel_case_types)]
            #[derive(::serde::Deserialize)]
            struct __add_params {
                a: int32,
                b: int32,
            }

            impl ::spider_tdl::Task for add {
                type Params = __add_params;
                type Return = (int32, int32);

                const NAME: &'static str = "add";

                fn execute(
                    ctx: ::spider_tdl::TaskContext,
                    params: Self::Params,
                ) -> ::std::result::Result<Self::Return, ::spider_tdl::TdlError> {
                    Self::__add(ctx, params.a, params.b)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn expand_task_empty_params_and_name_alias() {
        let actual = expand_to_string(
            r#"name = "my_ns::my_task""#,
            r"
            fn noop(ctx: TaskContext) -> Result<(int32,), TdlError> {
                Ok((42,))
            }
            ",
        );

        let expected = normalize(&quote! {
            #[allow(non_camel_case_types)]
            struct noop;

            impl noop {
                #[allow(dead_code, non_snake_case, clippy::needless_pass_by_value)]
                fn __assert_first_param_is_task_context(
                    ctx: TaskContext,
                ) -> ::spider_tdl::TaskContext {
                    ctx
                }

                #[allow(clippy::unnecessary_wraps)]
                fn __noop(
                    ctx: TaskContext
                ) -> ::std::result::Result<(int32,), ::spider_tdl::TdlError> {
                    Ok((42,))
                }
            }

            #[allow(non_camel_case_types)]
            #[derive(::serde::Deserialize)]
            struct __noop_params {}

            impl ::spider_tdl::Task for noop {
                type Params = __noop_params;
                type Return = (int32,);

                const NAME: &'static str = "my_ns::my_task";

                fn execute(
                    ctx: ::spider_tdl::TaskContext,
                    _params: Self::Params,
                ) -> ::std::result::Result<Self::Return, ::spider_tdl::TdlError> {
                    Self::__noop(ctx)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn auto_wrap_single_value_return() {
        let actual = expand_to_string(
            "",
            r"
            fn single(ctx: TaskContext, x: int32) -> Result<int32, TdlError> {
                let x = x * x;
                Ok(x)
            }
            ",
        );

        let expected = normalize(&quote! {
            #[allow(non_camel_case_types)]
            struct single;

            impl single {
                #[allow(dead_code, non_snake_case, clippy::needless_pass_by_value)]
                fn __assert_first_param_is_task_context(
                    ctx: TaskContext,
                ) -> ::spider_tdl::TaskContext {
                    ctx
                }

                #[allow(clippy::redundant_closure_call, clippy::unnecessary_wraps)]
                fn __single(ctx: TaskContext, x: int32)
                    -> ::std::result::Result<(int32,), ::spider_tdl::TdlError>
                {
                    (|| -> Result<int32, TdlError> {
                        let x = x * x;
                        Ok(x)
                    })().map(|__v| (__v,))
                }
            }

            #[allow(non_camel_case_types)]
            #[derive(::serde::Deserialize)]
            struct __single_params {
                x: int32,
            }

            impl ::spider_tdl::Task for single {
                type Params = __single_params;
                type Return = (int32,);

                const NAME: &'static str = "single";

                fn execute(
                    ctx: ::spider_tdl::TaskContext,
                    params: Self::Params,
                ) -> ::std::result::Result<Self::Return, ::spider_tdl::TdlError> {
                    Self::__single(ctx, params.x)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn reject_self_parameter() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn = syn::parse_str(
            "fn bad(&self, ctx: TaskContext) -> Result<(int32,), TdlError> { Ok((0,)) }",
        )
        .expect("failed to parse function");

        let err = expand(&attr, &func).expect_err("expected error for `self` parameter");
        assert!(err.to_string().contains("self"));
    }

    #[test]
    fn reject_no_parameters() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn = syn::parse_str("fn bad() -> Result<(int32,), TdlError> { Ok((42,)) }")
            .expect("failed to parse function");

        let err = expand(&attr, &func).expect_err("expected error for no parameters");
        assert!(err.to_string().contains("at least one parameter"));
    }

    #[test]
    fn reject_non_result_return() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn = syn::parse_str("fn bad(ctx: TaskContext) -> int32 { 42 }")
            .expect("failed to parse function");

        let err = expand(&attr, &func).expect_err("expected error for non-`Result` return");
        assert!(err.to_string().contains("Result"));
    }

    #[test]
    fn reject_unknown_attribute_argument() {
        let result: syn::Result<TaskAttr> = syn::parse_str("foo = \"bar\"");
        let Err(err) = result else {
            panic!("expected error for unknown attribute argument");
        };
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn reject_result_with_single_argument() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn = syn::parse_str("fn bad(ctx: TaskContext) -> Result<int32> { Ok(0) }")
            .expect("failed to parse function");

        let err = expand(&attr, &func)
            .expect_err("expected error for single-argument `Result` return type");
        assert!(err.to_string().contains("two generic arguments"));
    }
}
