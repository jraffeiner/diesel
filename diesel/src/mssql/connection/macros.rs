macro_rules! uint_enum {
    ($( #[$gattr:meta] )* pub enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* (pub(crate)) enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };
    ($( #[$gattr:meta] )* enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[allow(missing_docs)]
        uint_enum!($( #[$gattr ])* () enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };

    ($( #[$gattr:meta] )* ( $($vis:tt)* ) enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        $( #[$gattr] )*
        #[allow(missing_docs)]
        $( $vis )* enum $ty {
            $( $( #[$attr ])* $variant = $val, )*
        }

        impl ::std::convert::TryFrom<u8> for $ty {
            type Error = ();
            fn try_from(n: u8) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u8 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }

        impl ::std::convert::TryFrom<u32> for $ty {
            type Error = ();
            fn try_from(n: u32) -> ::std::result::Result<$ty, ()> {
                match n {
                    $( x if x == $ty::$variant as u32 => Ok($ty::$variant), )*
                    _ => Err(()),
                }
            }
        }
    }
}

macro_rules! to_sql {
    ($target:ident, $( $ty:ty: ($variant:expr, $val:expr) ;)* ) => {
        $(
            impl crate::mssql::connection::to_sql::ToSql for $ty {
                fn to_sql(&self) -> crate::mssql::connection::tds::codec::ColumnData<'_> {
                    let $target = self;
                    $variant(Some($val))
                }
            }

            impl crate::mssql::connection::to_sql::ToSql for Option<$ty> {
                fn to_sql(&self) -> crate::mssql::connection::tds::codec::ColumnData<'_> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }

            impl crate::mssql::connection::to_sql::ToSql for &Option<$ty> {
                fn to_sql(&self) -> crate::mssql::connection::tds::codec::ColumnData<'_> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }
        )*
    };
}

macro_rules! into_sql {
    ($target:ident, $( $ty:ty: ($variant:expr, $val:expr) ;)* ) => {
        $(
            impl<'a> crate::mssql::connection::to_sql::IntoSql<'a> for $ty {
                fn into_sql(self) -> crate::mssql::connection::tds::codec::ColumnData<'a> {
                    let $target = self;
                    $variant(Some($val))
                }
            }

            impl<'a> crate::mssql::connection::to_sql::IntoSql<'a> for Option<$ty> {
                fn into_sql(self) -> crate::mssql::connection::tds::codec::ColumnData<'a> {
                    match self {
                        Some(val) => {
                            let $target = val;
                            $variant(Some($val))
                        },
                        None => $variant(None)
                    }
                }
            }
        )*
    }
}

macro_rules! from_sql {
    ($( $ty:ty: $($pat:pat => ($borrowed_val:expr, $owned_val:expr)),* );* ) => {
        $(
            impl<'a> crate::mssql::connection::from_sql::FromSql<'a> for $ty {
                fn from_sql(data: &'a crate::mssql::connection::tds::codec::ColumnData<'a>) -> crate::mssql::connection::Result<Option<Self>> {
                    if data.is_null(){
                        return Ok(None)
                    }
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::mssql::connection::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }

            impl<'a> crate::mssql::connection::from_sql::FromSqlOwned<'a> for $ty {
                fn from_sql_owned(data: crate::mssql::connection::tds::codec::ColumnData<'a>) -> crate::mssql::connection::Result<Option<Self>> {
                    if data.is_null(){
                        return Ok(None)
                    }
                    match data {
                        $( $pat => Ok($owned_val), )*
                        _ => Err(crate::mssql::connection::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
    ($( $ty:ty: $($pat:pat => $borrowed_val:expr),* );* ) => {
        $(
            impl<'a> crate::mssql::connection::from_sql::FromSql<'a> for $ty {
                fn from_sql(data: &'a crate::mssql::connection::tds::codec::ColumnData<'a>) -> crate::mssql::connection::Result<Option<Self>> {
                    if data.is_null(){
                        return Ok(None)
                    }
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::mssql::connection::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }

            impl<'a> crate::mssql::connection::from_sql::FromSqlOwned<'a> for $ty {
                fn from_sql_owned(data: crate::mssql::connection::tds::codec::ColumnData<'a>) -> crate::mssql::connection::Result<Option<Self>> {
                    if data.is_null(){
                        return Ok(None)
                    }
                    match data {
                        $( $pat => Ok($borrowed_val), )*
                        _ => Err(crate::mssql::connection::Error::Conversion(format!("cannot interpret {:?} as an {} value", data, stringify!($ty)).into()))
                    }
                }
            }
        )*
    };
}
