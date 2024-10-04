use bip300301_messages::bitcoin::{Amount, OutPoint, TxOut};
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};

pub type Hash256 = [u8; 32];

#[derive(Debug, Serialize, Deserialize)]
pub struct Ctip {
    pub outpoint: OutPoint,
    pub value: Amount,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sidechain {
    pub sidechain_number: u8,
    pub data: Vec<u8>,
    pub vote_count: u16,
    pub proposal_height: u32,
    pub activation_height: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SidechainProposal {
    pub sidechain_number: u8,
    pub data: Vec<u8>,
    pub vote_count: u16,
    pub proposal_height: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingM6id {
    pub m6id: Hash256,
    pub vote_count: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TreasuryUtxo {
    pub outpoint: OutPoint,
    pub address: Option<Vec<u8>>,
    pub total_value: Amount,
    pub previous_total_value: Amount,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Deposit {
    pub sidechain_id: u8,
    pub sequence_number: u64,
    pub outpoint: OutPoint,
    pub output: TxOut,
}

#[derive(Clone, Copy, Debug)]
pub struct HeaderInfo {
    pub block_hash: Hash256,
    pub prev_block_hash: Hash256,
    pub height: u32,
    /// Total work as a uint256, little-endian
    pub work: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub enum WithdrawalBundleEventKind {
    Submitted,
    Failed,
    Succeeded,
}

#[derive(Clone, Debug)]
pub struct WithdrawalBundleEvent {
    pub sidechain_id: u8,
    pub m6id: Hash256,
    pub kind: WithdrawalBundleEventKind,
}

#[derive(Clone, Debug, Default)]
pub struct BlockInfo {
    pub deposits: Vec<Deposit>,
    pub withdrawal_bundle_events: Vec<WithdrawalBundleEvent>,
    /// Sequential map of sidechain IDs to BMM commitments
    pub bmm_commitments: LinkedHashMap<u8, Hash256>,
}

#[derive(Clone, Debug)]
pub enum Event {
    ConnectBlock {
        header_info: HeaderInfo,
        block_info: BlockInfo,
    },
    DisconnectBlock {
        block_hash: Hash256,
    },
}
