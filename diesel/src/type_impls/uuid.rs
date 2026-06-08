use crate::sql_types::Uuid;
use diesel_derives::{AsExpression, FromSqlRow};

#[derive(AsExpression, FromSqlRow)]
#[diesel(foreign_derive)]
#[diesel(sql_type = Uuid)]
#[expect(dead_code)]
struct UuidProxy(uuid::Uuid);
