mod mysql;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(QuotedEnumStr)]
pub fn derive_quoted_enum_str(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    mysql::derive_quoted_enum_str(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
