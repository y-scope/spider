use spider_derive::QuotedEnumStr;

#[derive(QuotedEnumStr)]
enum Color {
    Red,
    Green,
    Blue,
}

#[test]
fn variant_names_returns_comma_separated_names() {
    assert_eq!(Color::variant_names(), "Red, Green, Blue");
}

#[derive(QuotedEnumStr)]
enum Single {
    Only,
}

#[test]
fn single_variant() {
    assert_eq!(Single::variant_names(), "Only");
}
