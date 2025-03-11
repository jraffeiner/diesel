use super::schema::*;
use diesel::expression::is_aggregate;
use diesel::*;

#[diesel_test_helper::test]
fn boxed_queries_can_be_executed() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    insert_into(users::table)
        .values(&NewUser::new("Jim", None))
        .execute(connection)
        .unwrap();
    let query_which_fails_unless_all_segments_are_applied = users::table
        .select(users::name)
        .filter(users::name.ne("jim"))
        .order(users::name.desc())
        .limit(1)
        .offset(1)
        .into_boxed();

    let expected_data = vec!["Sean".to_string()];
    let data = query_which_fails_unless_all_segments_are_applied.load(connection);
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn boxed_queries_can_differ_conditionally() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    insert_into(users::table)
        .values(&NewUser::new("Jim", None))
        .execute(connection)
        .unwrap();

    enum Query {
        All,
        Ordered,
        One,
    }

    let source = |query| match query {
        Query::All => users::table.order(users::name.desc()).into_boxed(),
        Query::Ordered => users::table.order(users::name.desc()).into_boxed(),
        Query::One => users::table
            .filter(users::name.ne("jim"))
            .order(users::name.desc())
            .limit(1)
            .offset(1)
            .into_boxed(),
    };
    let sean = find_user_by_name("Sean", connection);
    let tess = find_user_by_name("Tess", connection);
    let jim = find_user_by_name("Jim", connection);

    let all = source(Query::All).load(connection);
    let expected_data = vec![tess.clone(), sean.clone(), jim.clone()];
    assert_eq!(Ok(expected_data), all);

    let ordered = source(Query::Ordered).load(connection);
    let expected_data = vec![tess, sean.clone(), jim];
    assert_eq!(Ok(expected_data), ordered);

    let one = source(Query::One).load(connection);
    let expected_data = vec![sean];
    assert_eq!(Ok(expected_data), one);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_select_dsl() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let data = users::table
        .into_boxed()
        .select(users::name)
        .order(users::name)
        .load::<String>(connection);
    assert_eq!(Ok(vec!["Sean".into(), "Tess".into()]), data);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_filter_dsl() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    insert_into(users::table)
        .values(&NewUser::new("Shane", None))
        .execute(connection)
        .unwrap();
    let data = users::table
        .into_boxed()
        .select(users::name)
        .filter(users::name.ne("Sean"))
        .filter(users::name.like("S%"))
        .load(connection);
    assert_eq!(Ok(vec![String::from("Shane")]), data);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_limit_dsl() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let data = users::table
        .into_boxed()
        .limit(1)
        .order(users::id)
        .load(connection);
    let expected_data = vec![find_user_by_name("Sean", connection)];
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_offset_dsl() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let data = users::table
        .into_boxed()
        .limit(1)
        .offset(1)
        .order(users::id)
        .load(connection);
    let expected_data = vec![find_user_by_name("Tess", connection)];
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_order_dsl() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let data = users::table
        .into_boxed()
        .order(users::name.desc())
        .load(connection);
    let expected_data = vec![
        find_user_by_name("Tess", connection),
        find_user_by_name("Sean", connection),
    ];
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn boxed_queries_can_use_borrowed_data() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let s = String::from("Sean");
    let data = users::table
        .into_boxed()
        .filter(users::name.eq(&s))
        .load(connection);
    let expected_data = vec![find_user_by_name("Sean", connection)];
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn queries_with_borrowed_data_can_be_boxed() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let s = String::from("Tess");
    let data = users::table
        .filter(users::name.eq(&s))
        .into_boxed()
        .load(connection);
    let expected_data = vec![find_user_by_name("Tess", connection)];
    assert_eq!(Ok(expected_data), data);
}

#[diesel_test_helper::test]
fn boxed_queries_implement_or_filter() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();
    let data = users::table
        .into_boxed()
        .filter(users::name.eq("Sean"))
        .or_filter(users::name.eq("Tess"))
        .order(users::name)
        .load(connection);
    let expected = vec![
        find_user_by_name("Sean", connection),
        find_user_by_name("Tess", connection),
    ];
    assert_eq!(Ok(expected), data);
}

#[diesel_test_helper::test]
fn can_box_query_with_boxable_expression() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();

    let expr: Box<dyn BoxableExpression<users::table, _, SqlType = _>> =
        Box::new(users::name.eq("Sean")) as _;

    let data = users::table.into_boxed().filter(expr).load(connection);
    let expected = vec![find_user_by_name("Sean", connection)];
    assert_eq!(Ok(expected), data);

    let expr: Box<dyn BoxableExpression<users::table, _, SqlType = _>> =
        Box::new(users::name.eq("Sean")) as _;

    let data = users::table.filter(expr).into_boxed().load(connection);
    let expected = vec![find_user_by_name("Sean", connection)];
    assert_eq!(Ok(expected), data);
}

#[diesel_test_helper::test]
fn can_box_query_having_with_boxable_expression() {
    let connection = &mut connection_with_sean_and_tess_in_users_table();

    let expr: Box<
        dyn BoxableExpression<
            users::table,
            crate::schema::TestBackend,
            users::id,
            is_aggregate::Yes,
            SqlType = _,
        >,
    > = Box::new(count(users::id).eq(1)) as _;

    use diesel::dsl::count;

    let data = users::table
        .select(count(users::id))
        .group_by(users::id)
        .having(expr)
        .load::<i64>(connection)
        .expect("db error");
    assert_eq!(data, [1, 1]);

    let expr: Box<
        dyn BoxableExpression<
            users::table,
            crate::schema::TestBackend,
            users::id,
            is_aggregate::Yes,
            SqlType = _,
        >,
    > = Box::new(count(users::id).eq(1)) as _;

    let data = users::table
        .select(count(users::id))
        .group_by(users::id)
        .into_boxed()
        .having(expr)
        .load::<i64>(connection)
        .expect("db error");
    assert_eq!(data, [1, 1]);
}
