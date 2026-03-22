use spider_derive::QuotedEnumStr;

#[allow(dead_code)]
#[derive(QuotedEnumStr)]
enum Color {
    Red,
    Green,
    Blue,
}

#[allow(dead_code)]
#[derive(QuotedEnumStr)]
enum Single {
    Only(i32),
}

#[test]
fn quoted_enum_str() {
    assert_eq!(Color::quoted_enum_str(), "'Red', 'Green', 'Blue'");
    assert_eq!(Single::quoted_enum_str(), "'Only'");
}
