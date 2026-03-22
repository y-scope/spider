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

impl PartialEq for Single {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Only(val1), Self::Only(val2)) => val1 == val2,
        }
    }
}

#[test]
fn test_quoted_enum_str() {
    assert_eq!(Color::quoted_enum_str(), "'Red', 'Green', 'Blue'");
    assert_eq!(Single::quoted_enum_str(), "'Only'");
}
