use diesel::associations::Identifiable;

table! {
    foos {
        id -> Integer,
    }
}

table! {
    bars {
        id -> VarChar,
    }
}

#[test]
fn derive_identifiable_on_simple_struct() {
    #[derive(Identifiable)]
    struct Foo {
        id: i32,
        #[expect(dead_code)]
        foo: i32,
    }

    let foo1 = Foo { id: 1, foo: 2 };
    let foo2 = Foo { id: 2, foo: 3 };
    assert_eq!(&1, foo1.id());
    assert_eq!(&2, foo2.id());
}

#[test]
fn derive_identifiable_on_tuple_struct() {
    #[derive(Identifiable)]
    struct Foo(
        #[diesel(column_name = id)] i32,
        #[expect(dead_code)]
        #[diesel(column_name = lol)]
        i32,
    );

    let foo1 = Foo(1, 2);
    let foo2 = Foo(2, 3);
    assert_eq!(&1, foo1.id());
    assert_eq!(&2, foo2.id());
}

#[test]
fn derive_identifiable_when_id_is_not_first_field() {
    #[derive(Identifiable)]
    struct Foo {
        #[expect(dead_code)]
        foo: i32,
        id: i32,
    }

    let foo1 = Foo { id: 1, foo: 2 };
    let foo2 = Foo { id: 2, foo: 3 };
    assert_eq!(&1, foo1.id());
    assert_eq!(&2, foo2.id());
}

#[test]
fn derive_identifiable_on_struct_with_non_integer_pk() {
    #[derive(Identifiable)]
    #[diesel(table_name = bars)]
    struct Foo {
        id: &'static str,
        #[expect(dead_code)]
        foo: i32,
    }

    let foo1 = Foo { id: "hi", foo: 2 };
    let foo2 = Foo {
        id: "there",
        foo: 3,
    };
    assert_eq!(&"hi", foo1.id());
    assert_eq!(&"there", foo2.id());
}

#[test]
fn derive_identifiable_on_struct_with_lifetime() {
    #[derive(Identifiable)]
    #[diesel(table_name = bars)]
    struct Foo<'a> {
        id: &'a str,
        #[expect(dead_code)]
        foo: i32,
    }

    let foo1 = Foo { id: "hi", foo: 2 };
    let foo2 = Foo {
        id: "there",
        foo: 3,
    };
    assert_eq!(&"hi", foo1.id());
    assert_eq!(&"there", foo2.id());
}

#[test]
fn derive_identifiable_with_non_standard_pk() {
    #[expect(dead_code)]
    #[derive(Identifiable)]
    #[diesel(table_name = bars)]
    #[diesel(primary_key(foo_id))]
    struct Foo<'a> {
        id: i32,
        foo_id: &'a str,
        foo: i32,
    }

    let foo1 = Foo {
        id: 1,
        foo_id: "hi",
        foo: 2,
    };
    let foo2 = Foo {
        id: 2,
        foo_id: "there",
        foo: 3,
    };
    assert_eq!(&"hi", foo1.id());
    assert_eq!(&"there", foo2.id());
}

#[test]
fn derive_identifiable_with_composite_pk() {
    #[expect(dead_code)]
    #[derive(Identifiable)]
    #[diesel(table_name = bars)]
    #[diesel(primary_key(foo_id, bar_id))]
    struct Foo {
        id: i32,
        foo_id: i32,
        bar_id: i32,
        foo: i32,
    }

    let foo1 = Foo {
        id: 1,
        foo_id: 2,
        bar_id: 3,
        foo: 4,
    };
    let foo2 = Foo {
        id: 5,
        foo_id: 6,
        bar_id: 7,
        foo: 8,
    };
    assert_eq!((&2, &3), foo1.id());
    assert_eq!((&6, &7), foo2.id());
}

#[test]
fn derive_identifiable_with_pk_serialize_as() {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MyI32(i32);

    impl From<i32> for MyI32 {
        fn from(value: i32) -> Self {
            MyI32(value)
        }
    }

    #[derive(Identifiable)]
    struct Foo {
        #[diesel(serialize_as = MyI32)]
        id: i32,
    }

    let foo1 = Foo { id: 1 };
    let foo2 = Foo { id: 2 };
    assert_eq!(MyI32(1), foo1.id());
    assert_eq!(MyI32(2), foo2.id());
}

#[test]
fn derive_identifiable_with_non_copy_pk_serialize() {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MyString(String);

    impl From<String> for MyString {
        fn from(value: String) -> Self {
            MyString(value)
        }
    }

    #[derive(Identifiable)]
    struct Foo {
        #[diesel(serialize_as = MyString)]
        id: String,
    }

    let foo1 = Foo { id: "1".to_owned() };
    let foo2 = Foo { id: "2".to_owned() };
    assert_eq!(MyString("1".to_owned()), foo1.id());
    assert_eq!(MyString("2".to_owned()), foo2.id());
}
