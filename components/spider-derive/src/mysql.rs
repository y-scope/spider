use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DataEnum, DeriveInput};

pub fn derive_quoted_enum_str(input: &DeriveInput) -> syn::Result<TokenStream> {
    let enum_type_name = &input.ident;

    let Data::Enum(DataEnum { variants, .. }) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "`QuotedEnumStr` can only be derived for enums",
        ));
    };

    let joined_quoted_enum_str: String = variants
        .iter()
        .map(|v| format!("'{variant}'", variant = v.ident))
        .collect::<Vec<String>>()
        .join(",");

    let expanded = quote! {
        impl #enum_type_name {
            pub const fn quoted_enum_str() -> &'static str {
                #joined_quoted_enum_str
            }
        }
    };

    Ok(expanded)
}
