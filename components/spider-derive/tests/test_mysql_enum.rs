use spider_derive::MySqlEnum;

#[allow(dead_code)]
#[derive(MySqlEnum)]
enum Color {
    Red,
    Green,
    Blue,
}

#[allow(dead_code)]
#[derive(MySqlEnum)]
enum Single {
    Only,
}

#[test]
fn test_mysql_enum_basic() {
    assert_eq!(Color::as_mysql_enum_decl(), "ENUM('Red','Green','Blue')");
    assert_eq!(Color::Red.as_str(), "Red");
    assert_eq!(Color::Green.as_str(), "Green");
    assert_eq!(Color::Blue.as_quoted_str(), "'Blue'");

    assert_eq!(Single::as_mysql_enum_decl(), "ENUM('Only')");
    assert_eq!(Single::Only.as_str(), "Only");
}
