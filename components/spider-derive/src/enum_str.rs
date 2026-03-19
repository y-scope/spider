use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DataEnum, DeriveInput};

pub fn derive_quoted_enum_str(input: &mut DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let values = match &input.data {
        Data::Enum(DataEnum { variants, .. }) => variants,
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "`QuotedEnumStr` can only be applied to an enum",
            ));
        }
    };

    let value_strings: Vec<String> = values.iter().map(|v| v.ident.to_string()).collect();

    let joined = value_strings.join(", ");

    let expanded = quote! {
        impl #name {
            pub fn variant_names() -> &'static str {
                #joined
            }
        }
    };

    Ok(expanded)
}
