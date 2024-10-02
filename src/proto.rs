use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid enum variant in field `{field_name}` of message `{message_name}`: `{variant_name}`")]
    InvalidEnumVariant {
        field_name: String,
        message_name: String,
        variant_name: String,
    },
    #[error("Invalid field value in field `{field_name}` of message `{message_name}`: `{value}`")]
    InvalidFieldValue {
        field_name: String,
        message_name: String,
        value: String,
    },
    #[error(
        "Invalid value in repeated field `{field_name}` of message `{message_name}`: `{value}`"
    )]
    InvalidRepeatedValue {
        field_name: String,
        message_name: String,
        value: String,
    },
    #[error("Missing field in message `{message_name}`: `{field_name}`")]
    MissingField {
        field_name: String,
        message_name: String,
    },
    #[error("Unknown enum tag in field `{field_name}` of message `{message_name}`: `{tag}`")]
    UnknownEnumTag {
        field_name: String,
        message_name: String,
        tag: i32,
    },
}

impl Error {
    pub fn invalid_enum_variant<Message>(field_name: &str, variant_name: &str) -> Self
    where
        Message: prost::Name,
    {
        Self::InvalidEnumVariant {
            field_name: field_name.to_owned(),
            message_name: Message::full_name(),
            variant_name: variant_name.to_owned(),
        }
    }

    pub fn invalid_field_value<Message>(field_name: &str, value: &str) -> Self
    where
        Message: prost::Name,
    {
        Self::InvalidFieldValue {
            field_name: field_name.to_owned(),
            message_name: Message::full_name(),
            value: value.to_owned(),
        }
    }

    pub fn invalid_repeated_value<Message>(field_name: &str, value: &str) -> Self
    where
        Message: prost::Name,
    {
        Self::InvalidRepeatedValue {
            field_name: field_name.to_owned(),
            message_name: Message::full_name(),
            value: value.to_owned(),
        }
    }

    pub fn missing_field<Message>(field_name: &str) -> Self
    where
        Message: prost::Name,
    {
        Self::MissingField {
            field_name: field_name.to_owned(),
            message_name: Message::full_name(),
        }
    }
}

pub mod mainchain {
    tonic::include_proto!("cusf.mainchain.v1");

    #[allow(unused_imports)]
    pub use mainchain_service_server::{
        self as server, MainchainService as Service, MainchainServiceServer as Server,
    };

    impl TryFrom<get_coinbase_psbt_request::ProposeSidechain> for bip300301_messages::CoinbaseMessage {
        type Error = super::Error;

        fn try_from(
            propose_sidechain: get_coinbase_psbt_request::ProposeSidechain,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::ProposeSidechain;
            let ProposeSidechain {
                sidechain_number,
                data,
            } = propose_sidechain;
            let sidechain_number: u8 = {
                let sidechain_number: u32 = sidechain_number.ok_or_else(|| {
                    Self::Error::missing_field::<ProposeSidechain>("sidechain_number")
                })?;
                sidechain_number.try_into().map_err(|_| {
                    Self::Error::invalid_field_value::<ProposeSidechain>(
                        "sidechain_number",
                        &sidechain_number.to_string(),
                    )
                })?
            };
            let data: Vec<u8> =
                data.ok_or_else(|| Self::Error::missing_field::<ProposeSidechain>("data"))?;
            Ok(bip300301_messages::CoinbaseMessage::M1ProposeSidechain {
                sidechain_number,
                data,
            })
        }
    }

    impl TryFrom<get_coinbase_psbt_request::AckSidechain> for bip300301_messages::CoinbaseMessage {
        type Error = super::Error;

        fn try_from(
            ack_sidechain: get_coinbase_psbt_request::AckSidechain,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::AckSidechain;
            let AckSidechain {
                sidechain_number,
                data_hash,
            } = ack_sidechain;
            let sidechain_number: u8 = {
                let sidechain_number: u32 = sidechain_number.ok_or_else(|| {
                    Self::Error::missing_field::<AckSidechain>("sidechain_number")
                })?;
                sidechain_number.try_into().map_err(|_| {
                    Self::Error::invalid_field_value::<AckSidechain>(
                        "sidechain_number",
                        &sidechain_number.to_string(),
                    )
                })?
            };
            let data_hash: [u8; 32] = {
                let data_hash = data_hash
                    .ok_or_else(|| Self::Error::missing_field::<AckSidechain>("data_hash"))?;
                data_hash.try_into().map_err(|data_hash| {
                    Self::Error::invalid_field_value::<AckSidechain>(
                        "data_hash",
                        &hex::encode(data_hash),
                    )
                })?
            };
            Ok(bip300301_messages::CoinbaseMessage::M2AckSidechain {
                sidechain_number,
                data_hash,
            })
        }
    }

    impl TryFrom<get_coinbase_psbt_request::ProposeBundle> for bip300301_messages::CoinbaseMessage {
        type Error = super::Error;

        fn try_from(
            propose_bundle: get_coinbase_psbt_request::ProposeBundle,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::ProposeBundle;
            let ProposeBundle {
                sidechain_number,
                bundle_txid,
            } = propose_bundle;
            let sidechain_number: u8 = {
                let sidechain_number: u32 = sidechain_number.ok_or_else(|| {
                    Self::Error::missing_field::<ProposeBundle>("sidechain_number")
                })?;
                sidechain_number.try_into().map_err(|_| {
                    Self::Error::invalid_field_value::<ProposeBundle>(
                        "sidechain_number",
                        &sidechain_number.to_string(),
                    )
                })?
            };
            let bundle_txid: [u8; 32] = {
                let data_hash = bundle_txid
                    .ok_or_else(|| Self::Error::missing_field::<ProposeBundle>("bundle_txid"))?;
                data_hash.try_into().map_err(|data_hash| {
                    Self::Error::invalid_field_value::<ProposeBundle>(
                        "bundle_txid",
                        &hex::encode(data_hash),
                    )
                })?
            };
            Ok(bip300301_messages::CoinbaseMessage::M3ProposeBundle {
                sidechain_number,
                bundle_txid,
            })
        }
    }

    impl From<get_coinbase_psbt_request::ack_bundles::RepeatPrevious>
        for bip300301_messages::M4AckBundles
    {
        fn from(repeat_previous: get_coinbase_psbt_request::ack_bundles::RepeatPrevious) -> Self {
            let get_coinbase_psbt_request::ack_bundles::RepeatPrevious {} = repeat_previous;
            Self::RepeatPrevious
        }
    }

    impl From<get_coinbase_psbt_request::ack_bundles::LeadingBy50>
        for bip300301_messages::M4AckBundles
    {
        fn from(leading_by_50: get_coinbase_psbt_request::ack_bundles::LeadingBy50) -> Self {
            let get_coinbase_psbt_request::ack_bundles::LeadingBy50 {} = leading_by_50;
            Self::LeadingBy50
        }
    }

    impl TryFrom<get_coinbase_psbt_request::ack_bundles::Upvotes> for bip300301_messages::M4AckBundles {
        type Error = super::Error;

        fn try_from(
            upvotes: get_coinbase_psbt_request::ack_bundles::Upvotes,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::ack_bundles::Upvotes;
            let Upvotes { upvotes } = upvotes;
            let mut two_bytes = false;
            for upvote in &upvotes {
                if *upvote > u16::MAX as u32 {
                    let err = Self::Error::invalid_repeated_value::<Upvotes>(
                        "upvotes",
                        &upvote.to_string(),
                    );
                    return Err(err);
                } else if *upvote > u8::MAX as u32 {
                    two_bytes = true;
                }
            }
            if two_bytes {
                let upvotes = upvotes.into_iter().map(|upvote| upvote as u16).collect();
                Ok(Self::TwoBytes { upvotes })
            } else {
                let upvotes = upvotes.into_iter().map(|upvote| upvote as u8).collect();
                Ok(Self::OneByte { upvotes })
            }
        }
    }

    impl TryFrom<get_coinbase_psbt_request::ack_bundles::AckBundles>
        for bip300301_messages::M4AckBundles
    {
        type Error = super::Error;

        fn try_from(
            ack_bundles: get_coinbase_psbt_request::ack_bundles::AckBundles,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::ack_bundles::AckBundles;
            match ack_bundles {
                AckBundles::LeadingBy50(leading_by_50) => Ok(leading_by_50.into()),
                AckBundles::RepeatPrevious(repeat_previous) => Ok(repeat_previous.into()),
                AckBundles::Upvotes(upvotes) => upvotes.try_into(),
            }
        }
    }

    impl TryFrom<get_coinbase_psbt_request::AckBundles> for bip300301_messages::M4AckBundles {
        type Error = super::Error;

        fn try_from(
            ack_bundles: get_coinbase_psbt_request::AckBundles,
        ) -> Result<Self, Self::Error> {
            use get_coinbase_psbt_request::AckBundles;
            let AckBundles { ack_bundles } = ack_bundles;
            ack_bundles
                .ok_or_else(|| Self::Error::missing_field::<AckBundles>("ack_bundles"))?
                .try_into()
        }
    }

    impl TryFrom<get_coinbase_psbt_request::AckBundles> for bip300301_messages::CoinbaseMessage {
        type Error = super::Error;

        fn try_from(
            ack_bundles: get_coinbase_psbt_request::AckBundles,
        ) -> Result<Self, Self::Error> {
            ack_bundles.try_into().map(Self::M4AckBundles)
        }
    }
}
