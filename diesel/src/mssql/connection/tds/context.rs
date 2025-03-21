use super::codec::*;
use std::sync::Arc;

/// Context, that might be required to make sure we understand and are understood by the server
#[derive(Debug)]
pub(crate) struct Context {
    version: FeatureLevel,
    packet_size: u32,
    packet_id: u8,
    transaction_desc: [u8; 8],
    last_meta: Option<Arc<TokenColMetaData<'static>>>,
    spn: Option<String>,
}

impl Context {
    pub(crate) fn new() -> Context {
        Context {
            version: FeatureLevel::SqlServerN,
            packet_size: 4096,
            packet_id: 0,
            transaction_desc: [0; 8],
            last_meta: None,
            spn: None,
        }
    }

    pub(crate) fn next_packet_id(&mut self) -> u8 {
        let id = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1);
        id
    }

    pub(crate) fn set_last_meta(&mut self, meta: Arc<TokenColMetaData<'static>>) {
        self.last_meta.replace(meta);
    }

    pub(crate) fn last_meta(&self) -> Option<Arc<TokenColMetaData<'static>>> {
        self.last_meta.clone()
    }

    pub(crate) fn packet_size(&self) -> u32 {
        self.packet_size
    }

    pub(crate) fn set_packet_size(&mut self, new_size: u32) {
        self.packet_size = new_size;
    }

    pub(crate) fn transaction_descriptor(&self) -> [u8; 8] {
        self.transaction_desc
    }

    pub(crate) fn set_transaction_descriptor(&mut self, desc: [u8; 8]) {
        self.transaction_desc = desc;
    }

    pub(crate) fn version(&self) -> FeatureLevel {
        self.version
    }

    pub(crate) fn set_spn(&mut self, host: impl AsRef<str>, port: u16) {
        self.spn = Some(format!("MSSQLSvc/{}:{}", host.as_ref(), port));
    }

    #[cfg(windows)]
    pub(crate) fn spn(&self) -> &str {
        self.spn.as_deref().unwrap_or("")
    }
}
