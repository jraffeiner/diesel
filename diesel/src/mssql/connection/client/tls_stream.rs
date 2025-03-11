use crate::mssql::connection::client::Config;
use std::io::{Read, Write};

#[cfg(feature = "native-tls")]
mod native_tls_stream;

#[cfg(feature = "rustls")]
mod rustls_tls_stream;

#[cfg(feature = "native-tls")]
pub(crate) use native_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) use rustls_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) fn create_tls_stream<S: Read + Write + Send>(
    config: &Config,
    stream: S,
) -> crate::mssql::connection::Result<TlsStream<S>> {
    TlsStream::new(config, stream)
}

#[cfg(feature = "native-tls")]
pub(crate) fn create_tls_stream<S: Read + Write + Send>(
    config: &Config,
    stream: S,
) -> crate::mssql::connection::Result<TlsStream<S>> {
    native_tls_stream::create_tls_stream(config, stream)
}
