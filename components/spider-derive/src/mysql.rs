use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DataEnum, DeriveInput};

pub fn derive_mysql_enum(input: &DeriveInput) -> syn::Result<TokenStream> {
    let enum_type_name = &input.ident;

    let Data::Enum(DataEnum { variants, .. }) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "`MySqlEnum` can only be derived for enums",
        ));
    };

    let joined_quoted_enum_str: String = variants
        .iter()
        .map(|v| format!("'{variant}'", variant = v.ident))
        .collect::<Vec<String>>()
        .join(",");
    let mysql_enum_decl = format!("ENUM({joined_quoted_enum_str})");

    let variant_name_arms = variants.iter().map(|v| {
        let ident = &v.ident;
        let variant_name = ident.to_string();
        quote! { #enum_type_name::#ident => #variant_name, }
    });

    let encode_arms = variant_name_arms.clone();

    let decode_arms = variants.iter().map(|v| {
        let ident = &v.ident;
        let variant_name = ident.to_string();
        quote! { #variant_name => Ok(#enum_type_name::#ident), }
    });

    let quoted_variant_names = variants.iter().map(|v| {
        let ident = &v.ident;
        let quoted_variant_name = format!("'{ident}'");
        quote! { #enum_type_name::#ident => #quoted_variant_name, }
    });

    let expanded = quote! {
        impl #enum_type_name {
            pub const fn as_mysql_enum_decl() -> &'static str {
                #mysql_enum_decl
            }

            pub const fn as_str(&self) -> &'static str {
                match self {
                    #(#variant_name_arms)*
                }
            }

            pub const fn as_quoted_str(&self) -> &'static str {
                match self {
                    #(#quoted_variant_names)*
                }
            }
        }

        impl sqlx::Type<sqlx::MySql> for #enum_type_name {
            fn type_info() -> sqlx::mysql::MySqlTypeInfo {
                <str as sqlx::Type<sqlx::MySql>>::type_info()
            }

            fn compatible(ty: &sqlx::mysql::MySqlTypeInfo) -> bool {
                <str as sqlx::Type<sqlx::MySql>>::compatible(ty)
            }
        }

        impl<'q> sqlx::Encode<'q, sqlx::MySql> for #enum_type_name {
            fn encode_by_ref(
                &self,
                buf: &mut <sqlx::MySql as sqlx::Database>::ArgumentBuffer<'q>,
            ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
                let s = match self {
                    #(#encode_arms)*
                };
                <&str as sqlx::Encode<'q, sqlx::MySql>>::encode_by_ref(&s, buf)
            }
        }


        impl<'r> sqlx::Decode<'r, sqlx::MySql> for #enum_type_name {
            fn decode(
                value: <sqlx::MySql as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <&str as sqlx::Decode<'r, sqlx::MySql>>::decode(value)?;
                match s {
                    #(#decode_arms)*
                    _ => Err(format!("unknown variant: {s}").into()),
                }
            }
        }
    };

    Ok(expanded)
}
