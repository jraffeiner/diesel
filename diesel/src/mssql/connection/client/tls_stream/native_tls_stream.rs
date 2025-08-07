use crate::mssql::connection::{
    client::{config::Config, TrustConfig},
    error::{Error, IoErrorKind},
};
pub(crate) use native_tls::TlsStream;
use native_tls::{Certificate, TlsConnector};
use std::fs;
use std::io::{Read, Write};
use tracing::{event, Level};

pub(crate) fn create_tls_stream<S: Read + Write + Send>(
    config: &Config,
    stream: S,
) -> crate::mssql::connection::Result<TlsStream<S>> {
    let mut builder = TlsConnector::builder();

    match &config.trust {
        TrustConfig::CaCertificateLocation(path) => {
            if let Ok(buf) = fs::read(path) {
                let cert = match path.extension() {
                        Some(ext)
                        if ext.to_ascii_lowercase() == "pem"
                            || ext.to_ascii_lowercase() == "crt" =>
                            {
                                Some(Certificate::from_pem(&buf)?)
                            }
                        Some(ext) if ext.to_ascii_lowercase() == "der" => {
                            Some(Certificate::from_der(&buf)?)
                        }
                        Some(_) | None => return Err(Error::Io {
                            kind: IoErrorKind::InvalidInput,
                            message: "Provided CA certificate with unsupported file-extension! Supported types are pem, crt and der.".to_string()}),
                    };
                if let Some(c) = cert {
                    builder.add_root_certificate(c);
                }
            } else {
                return Err(Error::Io {
                    kind: IoErrorKind::InvalidData,
                    message: "Could not read provided CA certificate!".to_string(),
                });
            }
        }
        TrustConfig::TrustAll => {
            event!(
                Level::INFO,
                "Trusting the server certificate without validation."
            );

            builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .use_sni(false);
        }
        TrustConfig::Default => {
            event!(Level::INFO, "Using default trust configuration.");
        }
    }

    let builder = builder.build()?;

    Ok(builder.connect(config.get_host(), stream)?)
}
