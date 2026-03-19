use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DataEnum, DeriveInput};

pub fn derive_quoted_enum_str(input: &DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let Data::Enum(DataEnum {
        variants: values, ..
    }) = &input.data
    else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "`QuotedEnumStr` can only be derived for enums",
        ));
    };

    let value_strings: Vec<String> = values
        .iter()
        .map(|v| format!("'{value_str}'", value_str = v.ident))
        .collect();

    let joined = value_strings.join(", ");

    let expanded = quote! {
        impl #name {
            pub const fn quoted_enum_str() -> &'static str {
                #joined
            }
        }
    };

    Ok(expanded)
}
