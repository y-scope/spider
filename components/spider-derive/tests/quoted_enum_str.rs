use spider_derive::QuotedEnumStr;

#[allow(dead_code)]
#[derive(QuotedEnumStr)]
enum Color {
    Red,
    Green,
    Blue,
}

#[test]
fn variant_names_returns_comma_separated_names() {
    assert_eq!(Color::quoted_enum_str(), "Red, Green, Blue");
}

#[allow(dead_code)]
#[derive(QuotedEnumStr)]
enum Single {
    Only,
}

#[test]
fn single_variant() {
    assert_eq!(Single::quoted_enum_str(), "Only");
}
