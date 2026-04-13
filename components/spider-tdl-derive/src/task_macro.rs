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
///
/// Supports an optional `name = "..."` argument. When omitted, the function name is used as the
/// task name.
pub struct TaskAttr {
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

/// Expands a `#[task]` annotated function into a marker struct, params struct, and `Task` trait
/// implementation.
pub fn expand(attr: &TaskAttr, func: &ItemFn) -> syn::Result<TokenStream> {
    validate_no_self(func)?;
    validate_has_parameters(func)?;

    let func_name = &func.sig.ident;
    let private_method_name = format_ident!("__{func_name}");
    let params_struct_name = format_ident!("__{func_name}_params");

    let task_name_str = attr
        .name
        .as_ref()
        .map_or_else(|| func_name.to_string(), LitStr::value);

    let first_param = func
        .sig
        .inputs
        .first()
        .expect("validated that function has at least one parameter");
    validate_first_param_is_task_context(first_param)?;

    let (return_type_tokens, needs_return_wrapping) = extract_return_tuple(&func.sig.output)?;

    let task_params: Vec<_> = func.sig.inputs.iter().skip(1).collect();

    let param_fields: Vec<TokenStream> = task_params
        .iter()
        .map(|arg| {
            let FnArg::Typed(pat_type) = arg else {
                unreachable!("self parameters are rejected by validation");
            };
            let pat = &pat_type.pat;
            let ty = &pat_type.ty;
            quote! { #pat: #ty }
        })
        .collect();

    let param_field_names: Vec<&Box<Pat>> = task_params
        .iter()
        .map(|arg| {
            let FnArg::Typed(pat_type) = arg else {
                unreachable!("self parameters are rejected by validation");
            };
            &pat_type.pat
        })
        .collect();

    let original_params = &func.sig.inputs;
    let original_output = &func.sig.output;
    let original_body = &func.block;
    let vis = &func.vis;

    let execute_call = if param_field_names.is_empty() {
        quote! { Self::#private_method_name(ctx) }
    } else {
        let field_accesses = param_field_names.iter().map(|name| {
            quote! { params.#name }
        });
        quote! { Self::#private_method_name(ctx, #(#field_accesses),*) }
    };

    let params_arg = if param_field_names.is_empty() {
        quote! { _params }
    } else {
        quote! { params }
    };

    let private_method = if needs_return_wrapping {
        let ReturnType::Type(_, original_return_type) = original_output else {
            unreachable!("validated that function has a return type");
        };
        let wrapped_return = quote! {
            Result<#return_type_tokens, spider_tdl::TdlError>
        };
        quote! {
            #[allow(clippy::redundant_closure_call)]
            fn #private_method_name(#original_params) -> #wrapped_return {
                (|| -> #original_return_type #original_body)().map(|__v| (__v,))
            }
        }
    } else {
        quote! {
            fn #private_method_name(#original_params) #original_output
                #original_body
        }
    };

    let expanded = quote! {
        #[allow(non_camel_case_types)]
        #vis struct #func_name;

        impl #func_name {
            #private_method
        }

        #[derive(serde::Deserialize)]
        struct #params_struct_name {
            #(#param_fields,)*
        }

        impl spider_tdl::Task for #func_name {
            const NAME: &'static str = #task_name_str;
            type Params = #params_struct_name;
            type Return = #return_type_tokens;

            fn execute(
                ctx: spider_tdl::TaskContext,
                #params_arg: Self::Params,
            ) -> Result<Self::Return, spider_tdl::TdlError> {
                #execute_call
            }
        }
    };

    Ok(expanded)
}

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

fn validate_has_parameters(func: &ItemFn) -> syn::Result<()> {
    if func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "task functions must have at least one parameter (TaskContext)",
        ));
    }
    Ok(())
}

fn validate_first_param_is_task_context(param: &FnArg) -> syn::Result<()> {
    let FnArg::Typed(pat_type) = param else {
        return Err(syn::Error::new_spanned(
            param,
            "first parameter must be `TaskContext`, not `self`",
        ));
    };

    let Type::Path(type_path) = pat_type.ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            &pat_type.ty,
            "first parameter must have type `TaskContext`",
        ));
    };

    let last_segment = type_path
        .path
        .segments
        .last()
        .expect("type path should have at least one segment");

    if last_segment.ident != "TaskContext" {
        return Err(syn::Error::new_spanned(
            &pat_type.ty,
            "first parameter must have type `TaskContext`",
        ));
    }

    Ok(())
}

/// Returns `(return_type_tokens, needs_wrapping)` where `needs_wrapping` is `true` when the
/// user wrote a bare type (e.g., `int32`) that was auto-wrapped into `(int32,)`.
fn extract_return_tuple(output: &ReturnType) -> syn::Result<(TokenStream, bool)> {
    let ReturnType::Type(_, return_type) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "task functions must return `Result<(T, ...), TdlError>`",
        ));
    };

    let Type::Path(type_path) = return_type.as_ref() else {
        return Err(syn::Error::new_spanned(
            return_type,
            "task functions must return `Result<(T, ...), TdlError>`",
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
            "task functions must return `Result<(T, ...), TdlError>`",
        ));
    }

    let PathArguments::AngleBracketed(angle_args) = &last_segment.arguments else {
        return Err(syn::Error::new_spanned(
            &last_segment.arguments,
            "expected generic arguments on `Result`",
        ));
    };

    let first_arg = angle_args.args.first().ok_or_else(|| {
        syn::Error::new_spanned(
            angle_args,
            "expected at least one generic argument on `Result`",
        )
    })?;

    let GenericArgument::Type(ok_type) = first_arg else {
        return Err(syn::Error::new_spanned(
            first_arg,
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

    /// Parses `attr_str` as task attributes, `func_str` as a function, expands, and returns the
    /// normalized token string.
    fn expand_to_string(attr_str: &str, func_str: &str) -> String {
        let attr: TaskAttr = syn::parse_str(attr_str).expect("failed to parse task attribute");
        let func: ItemFn = syn::parse_str(func_str).expect("failed to parse function");
        let expanded = expand(&attr, &func).expect("macro expansion failed");
        expanded.to_string()
    }

    /// Normalizes a `TokenStream` to a comparable string.
    fn normalize(tokens: &TokenStream) -> String {
        tokens.to_string()
    }

    #[test]
    fn expand_task_with_tuple_return() {
        let actual = expand_to_string(
            "",
            r"
            fn add(ctx: TaskContext, a: int32, b: int32) -> Result<(int32, int32), TdlError> {
                Ok((a + b, a - b))
            }
            ",
        );

        let expected = normalize(&quote! {
            #[allow(non_camel_case_types)]
            struct add;

            impl add {
                fn __add(ctx: TaskContext, a: int32, b: int32) -> Result<(int32, int32), TdlError> {
                    Ok((a + b, a - b))
                }
            }

            #[derive(serde::Deserialize)]
            struct __add_params {
                a: int32,
                b: int32,
            }

            impl spider_tdl::Task for add {
                const NAME: &'static str = "add";
                type Params = __add_params;
                type Return = (int32, int32);

                fn execute(
                    ctx: spider_tdl::TaskContext,
                    params: Self::Params,
                ) -> Result<Self::Return, spider_tdl::TdlError> {
                    Self::__add(ctx, params.a, params.b)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn expand_task_with_custom_name() {
        let actual = expand_to_string(
            r#"name = "my_ns::my_task""#,
            r"
            fn my_task(ctx: TaskContext, x: int64) -> Result<(int64,), TdlError> {
                Ok((x,))
            }
            ",
        );

        assert!(actual.contains(r#"const NAME : & 'static str = "my_ns::my_task""#));
    }

    #[test]
    fn expand_task_empty_params() {
        let actual = expand_to_string(
            "",
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
                fn __noop(ctx: TaskContext) -> Result<(int32,), TdlError> {
                    Ok((42,))
                }
            }

            #[derive(serde::Deserialize)]
            struct __noop_params {}

            impl spider_tdl::Task for noop {
                const NAME: &'static str = "noop";
                type Params = __noop_params;
                type Return = (int32,);

                fn execute(
                    ctx: spider_tdl::TaskContext,
                    _params: Self::Params,
                ) -> Result<Self::Return, spider_tdl::TdlError> {
                    Self::__noop(ctx)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn reject_missing_task_context() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn =
            syn::parse_str("fn bad(a: int32) -> Result<(int32,), TdlError> { Ok((a,)) }")
                .expect("failed to parse function");

        let err = expand(&attr, &func).expect_err("expected error for missing TaskContext");
        assert!(err.to_string().contains("TaskContext"));
    }

    #[test]
    fn auto_wrap_single_value_return() {
        let actual = expand_to_string(
            "",
            r"
            fn single(ctx: TaskContext, x: int32) -> Result<int32, TdlError> {
                Ok(x)
            }
            ",
        );

        let expected = normalize(&quote! {
            #[allow(non_camel_case_types)]
            struct single;

            impl single {
                #[allow(clippy::redundant_closure_call)]
                fn __single(ctx: TaskContext, x: int32) -> Result<(int32,), spider_tdl::TdlError> {
                    (|| -> Result<int32, TdlError> { Ok(x) })().map(|__v| (__v,))
                }
            }

            #[derive(serde::Deserialize)]
            struct __single_params {
                x: int32,
            }

            impl spider_tdl::Task for single {
                const NAME: &'static str = "single";
                type Params = __single_params;
                type Return = (int32,);

                fn execute(
                    ctx: spider_tdl::TaskContext,
                    params: Self::Params,
                ) -> Result<Self::Return, spider_tdl::TdlError> {
                    Self::__single(ctx, params.x)
                }
            }
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn reject_no_parameters() {
        let attr: TaskAttr = syn::parse_str("").expect("failed to parse attribute");
        let func: ItemFn = syn::parse_str("fn bad() -> Result<(int32,), TdlError> { Ok((42,)) }")
            .expect("failed to parse function");

        let err = expand(&attr, &func).expect_err("expected error for no parameters");
        assert!(err.to_string().contains("at least one parameter"));
    }
}
