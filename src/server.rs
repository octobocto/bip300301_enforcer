use crate::proto::{
    self,
    mainchain::{
        get_ctip_response::Ctip, get_sidechain_proposals_response::SidechainProposal,
        get_sidechains_response::SidechainInfo, server::MainchainService,
        BroadcastWithdrawalBundleRequest, BroadcastWithdrawalBundleResponse,
        CreateBmmCriticalDataTransactionRequest, CreateBmmCriticalDataTransactionResponse,
        CreateDepositTransactionRequest, CreateDepositTransactionResponse, CreateNewAddressRequest,
        CreateNewAddressResponse, GenerateBlocksRequest, GenerateBlocksResponse,
        GetBlockHeaderInfoRequest, GetBlockHeaderInfoResponse, GetBlockInfoRequest,
        GetBlockInfoResponse, GetBmmHStarCommitmentsRequest, GetBmmHStarCommitmentsResponse,
        GetChainInfoRequest, GetChainInfoResponse, GetChainTipRequest, GetChainTipResponse,
        GetCoinbasePsbtRequest, GetCoinbasePsbtResponse, GetCtipRequest, GetCtipResponse,
        GetSidechainProposalsRequest, GetSidechainProposalsResponse, GetSidechainsRequest,
        GetSidechainsResponse, GetTwoWayPegDataRequest, GetTwoWayPegDataResponse,
        SubscribeEventsRequest, SubscribeEventsResponse,
    },
};

use bip300301_messages::{
    bitcoin::{self, absolute::Height, hashes::Hash, Transaction, TxOut},
    CoinbaseMessage,
};
use miette::Result;
use tonic::{Request, Response, Status};

pub use crate::bip300::Bip300;
use crate::types;

fn invalid_field_value<Message>(field_name: &str, value: &str) -> tonic::Status
where
    Message: prost::Name,
{
    let err = proto::Error::invalid_field_value::<Message>(field_name, value);
    tonic::Status::invalid_argument(err.to_string())
}

fn missing_field<Message>(field_name: &str) -> tonic::Status
where
    Message: prost::Name,
{
    let err = proto::Error::missing_field::<Message>(field_name);
    tonic::Status::invalid_argument(err.to_string())
}

#[tonic::async_trait]
impl MainchainService for Bip300 {
    async fn broadcast_withdrawal_bundle(
        &self,
        _request: tonic::Request<BroadcastWithdrawalBundleRequest>,
    ) -> Result<tonic::Response<BroadcastWithdrawalBundleResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn create_bmm_critical_data_transaction(
        &self,
        _request: tonic::Request<CreateBmmCriticalDataTransactionRequest>,
    ) -> Result<tonic::Response<CreateBmmCriticalDataTransactionResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn create_deposit_transaction(
        &self,
        _request: tonic::Request<CreateDepositTransactionRequest>,
    ) -> Result<tonic::Response<CreateDepositTransactionResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn create_new_address(
        &self,
        _request: tonic::Request<CreateNewAddressRequest>,
    ) -> std::result::Result<tonic::Response<CreateNewAddressResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    /// Regtest only
    async fn generate_blocks(
        &self,
        _request: tonic::Request<GenerateBlocksRequest>,
    ) -> Result<tonic::Response<GenerateBlocksResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }
    async fn get_block_header_info(
        &self,
        _request: tonic::Request<GetBlockHeaderInfoRequest>,
    ) -> Result<tonic::Response<GetBlockHeaderInfoResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn get_block_info(
        &self,
        _request: tonic::Request<GetBlockInfoRequest>,
    ) -> Result<tonic::Response<GetBlockInfoResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn get_bmm_h_star_commitments(
        &self,
        _request: tonic::Request<GetBmmHStarCommitmentsRequest>,
    ) -> Result<tonic::Response<GetBmmHStarCommitmentsResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn get_chain_info(
        &self,
        _request: tonic::Request<GetChainInfoRequest>,
    ) -> Result<tonic::Response<GetChainInfoResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn get_chain_tip(
        &self,
        _request: tonic::Request<GetChainTipRequest>,
    ) -> Result<tonic::Response<GetChainTipResponse>, tonic::Status> {
        // FIXME: implement
        todo!()
    }

    async fn get_coinbase_psbt(
        &self,
        request: Request<GetCoinbasePsbtRequest>,
    ) -> Result<Response<GetCoinbasePsbtResponse>, Status> {
        let request = request.into_inner();
        let mut messages = Vec::<CoinbaseMessage>::new();
        for propose_sidechain in request.propose_sidechains {
            let message = propose_sidechain
                .try_into()
                .map_err(|err: proto::Error| tonic::Status::invalid_argument(err.to_string()))?;
            messages.push(message);
        }
        for ack_sidechain in request.ack_sidechains {
            let message = ack_sidechain
                .try_into()
                .map_err(|err: proto::Error| tonic::Status::invalid_argument(err.to_string()))?;
            messages.push(message);
        }
        for propose_bundle in request.propose_bundles {
            let message = propose_bundle
                .try_into()
                .map_err(|err: proto::Error| tonic::Status::invalid_argument(err.to_string()))?;
            messages.push(message);
        }
        let ack_bundles = request
            .ack_bundles
            .ok_or_else(|| missing_field::<GetCoinbasePsbtRequest>("ack_bundles"))?;
        {
            let message = ack_bundles
                .try_into()
                .map_err(|err: proto::Error| tonic::Status::invalid_argument(err.to_string()))?;
            messages.push(message);
        }
        let output = messages
            .into_iter()
            .map(|message| TxOut {
                value: 0,
                script_pubkey: message.into(),
            })
            .collect();
        let transasction = Transaction {
            output,
            input: vec![],
            lock_time: bitcoin::absolute::LockTime::Blocks(Height::ZERO),
            version: 2,
        };
        let psbt = bitcoin::consensus::serialize(&transasction);
        let response = GetCoinbasePsbtResponse { psbt };
        Ok(Response::new(response))
    }

    async fn get_ctip(
        &self,
        request: tonic::Request<GetCtipRequest>,
    ) -> Result<tonic::Response<GetCtipResponse>, tonic::Status> {
        let GetCtipRequest { sidechain_number } = request.into_inner();
        let sidechain_number: u8 = {
            let sidechain_number: u32 = sidechain_number
                .ok_or_else(|| missing_field::<GetCtipRequest>("sidechain_number"))?;
            sidechain_number.try_into().map_err(|_| {
                invalid_field_value::<GetCtipRequest>(
                    "sidechain_number",
                    &sidechain_number.to_string(),
                )
            })?
        };
        let ctip = self
            .get_ctip(sidechain_number)
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        if let Some(ctip) = ctip {
            let sequence_number = self
                .get_ctip_sequence_number(sidechain_number)
                .map_err(|err| tonic::Status::internal(err.to_string()))?;
            // get_ctip returned Some(ctip) above, so we know that the sequence_number will also
            // return Some, so we just unwrap it.
            let sequence_number = sequence_number.unwrap();
            let ctip = Ctip {
                txid: ctip.outpoint.txid.as_byte_array().into(),
                vout: ctip.outpoint.vout,
                value: ctip.value,
                sequence_number,
            };
            let response = GetCtipResponse { ctip: Some(ctip) };
            Ok(Response::new(response))
        } else {
            let response = GetCtipResponse { ctip: None };
            Ok(Response::new(response))
        }
    }

    /*
    async fn get_deposits(
        &self,
        request: Request<GetDepositsRequest>,
    ) -> Result<Response<GetDepositsResponse>, Status> {
        let request = request.into_inner();
        let sidechain_number = request.sidechain_number as u8;
        let deposits = self.get_deposits(sidechain_number).unwrap();
        let mut response = GetDepositsResponse { deposits: vec![] };
        for deposit in deposits {
            let deposit = Deposit {
                address: deposit.address,
                value: deposit.value,
                sequence_number: deposit.sequence_number,
            };
            response.deposits.push(deposit);
        }
        Ok(Response::new(response))
    }
    */

    async fn get_sidechain_proposals(
        &self,
        request: tonic::Request<GetSidechainProposalsRequest>,
    ) -> Result<tonic::Response<GetSidechainProposalsResponse>, tonic::Status> {
        let GetSidechainProposalsRequest {} = request.into_inner();
        let sidechain_proposals = self
            .get_sidechain_proposals()
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let sidechain_proposals = sidechain_proposals
            .into_iter()
            .map(
                |(
                    data_hash,
                    types::SidechainProposal {
                        sidechain_number,
                        data,
                        vote_count,
                        proposal_height,
                    },
                )| {
                    SidechainProposal {
                        sidechain_number: sidechain_number as u32,
                        data,
                        data_hash: data_hash.to_vec(),
                        vote_count: vote_count as u32,
                        proposal_height,
                        proposal_age: 0,
                    }
                },
            )
            .collect();
        let response = GetSidechainProposalsResponse {
            sidechain_proposals,
        };
        Ok(Response::new(response))
    }

    async fn get_sidechains(
        &self,
        request: tonic::Request<GetSidechainsRequest>,
    ) -> Result<tonic::Response<GetSidechainsResponse>, tonic::Status> {
        let GetSidechainsRequest {} = request.into_inner();
        let sidechains = self
            .get_sidechains()
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let sidechains = sidechains
            .into_iter()
            .map(|sidechain| {
                let types::Sidechain {
                    sidechain_number,
                    data,
                    vote_count,
                    proposal_height,
                    activation_height,
                } = sidechain;
                SidechainInfo {
                    sidechain_number: sidechain_number as u32,
                    data,
                    vote_count: vote_count as u32,
                    proposal_height,
                    activation_height,
                }
            })
            .collect();
        let response = GetSidechainsResponse { sidechains };
        Ok(Response::new(response))
    }

    async fn get_two_way_peg_data(
        &self,
        request: tonic::Request<GetTwoWayPegDataRequest>,
    ) -> Result<tonic::Response<GetTwoWayPegDataResponse>, tonic::Status> {
        let GetTwoWayPegDataRequest {
            sidechain_id,
            start_block_hash,
            end_block_hash,
        } = request.into_inner();
        let _sidechain_id: u8 = {
            let sidechain_id: u32 = sidechain_id
                .ok_or_else(|| missing_field::<GetTwoWayPegDataRequest>("sidechain_id"))?;
            sidechain_id.try_into().map_err(|_| {
                invalid_field_value::<GetTwoWayPegDataRequest>(
                    "sidechain_id",
                    &sidechain_id.to_string(),
                )
            })?
        };
        let _start_block_hash: Option<[u8; 32]> = start_block_hash
            .map(|start_block_hash| {
                start_block_hash.try_into().map_err(|start_block_hash| {
                    invalid_field_value::<GetTwoWayPegDataRequest>(
                        "start_block_hash",
                        &hex::encode(start_block_hash),
                    )
                })
            })
            .transpose()?;
        let _end_block_hash: [u8; 32] = end_block_hash
            .ok_or_else(|| missing_field::<GetTwoWayPegDataRequest>("end_block_hash"))?
            .try_into()
            .map_err(|end_block_hash| {
                invalid_field_value::<GetTwoWayPegDataRequest>(
                    "end_block_hash",
                    &hex::encode(end_block_hash),
                )
            })?;
        // FIXME: implement
        todo!()
    }

    type SubscribeEventsStream =
        futures::channel::mpsc::UnboundedReceiver<Result<SubscribeEventsResponse, tonic::Status>>;

    async fn subscribe_events(
        &self,
        request: tonic::Request<SubscribeEventsRequest>,
    ) -> Result<tonic::Response<Self::SubscribeEventsStream>, tonic::Status> {
        let SubscribeEventsRequest { sidechain_id } = request.into_inner();
        let _sidechain_id: u8 = {
            let sidechain_id: u32 = sidechain_id
                .ok_or_else(|| missing_field::<GetTwoWayPegDataRequest>("sidechain_id"))?;
            sidechain_id.try_into().map_err(|_| {
                invalid_field_value::<GetTwoWayPegDataRequest>(
                    "sidechain_id",
                    &sidechain_id.to_string(),
                )
            })?
        };
        // FIXME: implement
        todo!()
    }

    /*
    async fn get_main_block_height(
        &self,
        _request: tonic::Request<GetMainBlockHeightRequest>,
    ) -> std::result::Result<tonic::Response<GetMainBlockHeightResponse>, tonic::Status> {
        let height = self.get_main_block_height().unwrap();
        let response = GetMainBlockHeightResponse { height };
        Ok(Response::new(response))
    }

    async fn get_main_chain_tip(
        &self,
        _request: tonic::Request<GetMainChainTipRequest>,
    ) -> std::result::Result<tonic::Response<GetMainChainTipResponse>, tonic::Status> {
        let block_hash = self.get_main_chain_tip().unwrap();
        let response = GetMainChainTipResponse {
            block_hash: block_hash.to_vec(),
        };
        Ok(Response::new(response))
    }
    */

    // This is commented out for now, because it references Protobuf messages that
    // does not exist.
    // async fn get_accepted_bmm_hashes(
    //     &self,
    //     _request: Request<GetAcceptedBmmHashesRequest>,
    // ) -> std::result::Result<tonic::Response<GetAcceptedBmmHashesResponse>, tonic::Status> {
    //     let accepted_bmm_hashes = self.get_accepted_bmm_hashes().unwrap();
    //     let accepted_bmm_hashes = accepted_bmm_hashes
    //         .into_iter()
    //         .map(|(block_height, bmm_hashes)| {
    //             let bmm_hashes = bmm_hashes
    //                 .into_iter()
    //                 .map(|bmm_hash| bmm_hash.to_vec())
    //                 .collect();
    //             BlockHeightBmmHashes {
    //                 block_height,
    //                 bmm_hashes,
    //             }
    //         })
    //         .collect();
    //     let response = GetAcceptedBmmHashesResponse {
    //         accepted_bmm_hashes,
    //     };
    //     Ok(Response::new(response))
    // }
}
