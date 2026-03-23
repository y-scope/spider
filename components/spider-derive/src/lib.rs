mod mysql;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(MySqlEnum)]
pub fn derive_mysql_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    mysql::derive_mysql_enum(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
