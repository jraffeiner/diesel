use crate::mssql::connection::{SqlReadBytes, FEA_EXT_FEDAUTH, FEA_EXT_TERMINATOR};

#[derive(Debug)]
pub(crate) struct TokenFeatureExtAck {
    pub features: Vec<FeatureAck>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum FedAuthAck {
    SecurityToken { nonce: Option<[u8; 32]> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum FeatureAck {
    FedAuth(FedAuthAck),
}

impl TokenFeatureExtAck {
    pub(crate) fn decode<R>(src: &mut R) -> crate::mssql::connection::Result<Self>
    where
        R: SqlReadBytes,
    {
        let mut features = Vec::new();
        loop {
            let feature_id = src.read_u8()?;

            if feature_id == FEA_EXT_TERMINATOR {
                break;
            } else if feature_id == FEA_EXT_FEDAUTH {
                let data_len = src.read_u32_le()?;

                let nonce = if data_len == 32 {
                    let mut n = [0u8; 32];
                    src.read_exact(&mut n)?;

                    Some(n)
                } else if data_len == 0 {
                    None
                } else {
                    panic!("invalid Feature_Ext_Ack token");
                };

                features.push(FeatureAck::FedAuth(FedAuthAck::SecurityToken { nonce }))
            } else {
                unimplemented!("unsupported feature {}", feature_id)
            }
        }

        Ok(TokenFeatureExtAck { features })
    }
}
