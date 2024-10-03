use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::Path,
    str::FromStr,
    time::Duration,
};

use bip300301_messages::{
    bitcoin::{self, hashes::Hash as _},
    m6_to_id, parse_coinbase_script, parse_m8_bmm_request, parse_op_drivechain, sha256d,
    CoinbaseMessage, M4AckBundles, ABSTAIN_TWO_BYTES, ALARM_TWO_BYTES,
};
use bitcoin::{
    consensus::Decodable, opcodes::all::OP_RETURN, Block, BlockHash, OutPoint, Transaction,
};
use fallible_iterator::FallibleIterator;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use miette::{miette, IntoDiagnostic};
use thiserror::Error;
use tokio::{
    task::JoinHandle,
    time::{interval, Instant},
};
use tokio_stream::wrappers::IntervalStream;
use ureq_jsonrpc::{json, Client};

use crate::{
    cli::Config,
    types::{Ctip, Hash256, PendingM6id, Sidechain, SidechainProposal, TreasuryUtxo},
};

mod dbs;

use dbs::{
    CommitWriteTxnError, CreateDbsError, DbDeleteError, DbFirstError, DbGetError, DbIterError,
    DbLenError, DbPutError, Dbs, RwTxn, UnitKey, WriteTxnError,
};

/*
const WITHDRAWAL_BUNDLE_MAX_AGE: u16 = 26_300;
const WITHDRAWAL_BUNDLE_INCLUSION_THRESHOLD: u16 = WITHDRAWAL_BUNDLE_MAX_AGE / 2; // 13_150, aka 51% hashrate

const USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = WITHDRAWAL_BUNDLE_MAX_AGE; // 26_300
const USED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 = USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE / 2;

const UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = 2016;
const UNUSED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 = UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE / 2; // 1008, aka 51% hashrate

*/

const WITHDRAWAL_BUNDLE_MAX_AGE: u16 = 10;
const WITHDRAWAL_BUNDLE_INCLUSION_THRESHOLD: u16 = WITHDRAWAL_BUNDLE_MAX_AGE / 2; // 5

const USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = WITHDRAWAL_BUNDLE_MAX_AGE; // 5
const USED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 = USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE / 2;

const UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = 10;
const UNUSED_SIDECHAIN_SLOT_ACTIVATION_MAX_FAILS: u16 = 5;
const UNUSED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 =
    UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE - UNUSED_SIDECHAIN_SLOT_ACTIVATION_MAX_FAILS;

#[derive(Debug, Error)]
enum HandleM1ProposeSidechainError {
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
}

#[derive(Debug, Error)]
enum HandleM2AckSidechainError {
    #[error(transparent)]
    DbDelete(#[from] DbDeleteError),
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
}

#[derive(Debug, Error)]
enum HandleFailedSidechainProposalsError {
    #[error(transparent)]
    DbDelete(#[from] DbDeleteError),
    #[error(transparent)]
    DbIter(#[from] DbIterError),
    #[error(transparent)]
    DbGet(#[from] DbGetError),
}

#[derive(Debug, Error)]
enum HandleM3ProposeBundleError {
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
    #[error("Cannot propose bundle; sidechain slot {sidechain_number} is inactive")]
    InactiveSidechain { sidechain_number: u8 },
}

#[derive(Debug, Error)]
enum HandleM4VotesError {
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
}

#[derive(Debug, Error)]
enum HandleM4AckBundlesError {
    #[error("Error handling M4 Votes")]
    Votes(#[from] HandleM4VotesError),
}

#[derive(Debug, Error)]
enum HandleFailedM6IdsError {
    #[error(transparent)]
    DbIter(#[from] DbIterError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
}

#[derive(Debug, Error)]
enum HandleM5M6Error {
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
    #[error("Invalid M6")]
    InvalidM6,
    #[error("Old Ctip for sidechain {sidechain_number} is unspent")]
    OldCtipUnspent { sidechain_number: u8 },
}

#[derive(Debug, Error)]
enum HandleM8Error {
    #[error("BMM request expired")]
    BmmRequestExpired,
    #[error("Cannot include BMM request; not accepted by miners")]
    NotAcceptedByMiners,
}

#[derive(Debug, Error)]
enum ConnectBlockError {
    #[error(transparent)]
    DbDelete(#[from] DbDeleteError),
    #[error(transparent)]
    DbFirst(#[from] DbFirstError),
    #[error(transparent)]
    DbLen(#[from] DbLenError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
    #[error("Error handling failed M6IDs")]
    FailedM6Ids(#[from] HandleFailedM6IdsError),
    #[error("Error handling failed sidechain proposals")]
    FailedSidechainProposals(#[from] HandleFailedSidechainProposalsError),
    #[error("Error handling M1 (propose sidechain)")]
    M1ProposeSidechain(#[from] HandleM1ProposeSidechainError),
    #[error("Error handling M2 (ack sidechain)")]
    M2AckSidechain(#[from] HandleM2AckSidechainError),
    #[error("Error handling M3 (propose bundle)")]
    M3ProposeBundle(#[from] HandleM3ProposeBundleError),
    #[error("Error handling M4 (ack bundles)")]
    M4AckBundles(#[from] HandleM4AckBundlesError),
    #[error("Error handling M5/M6")]
    M5M6(#[from] HandleM5M6Error),
    #[error("Error handling M8")]
    M8(#[from] HandleM8Error),
    #[error("Multiple blocks BMM'd in sidechain slot {sidechain_number}")]
    MultipleBmmBlocks { sidechain_number: u8 },
}

#[derive(Debug, Error)]
enum DisconnectBlockError {}

#[derive(Debug, Error)]
enum TxValidationError {}

#[derive(Debug, Error)]
enum InitialSyncError {
    #[error(transparent)]
    CommitWriteTxn(#[from] CommitWriteTxnError),
    #[error("Failed to connect block")]
    ConnectBlock(#[from] ConnectBlockError),
    #[error(transparent)]
    DbGet(#[from] DbGetError),
    #[error(transparent)]
    DbPut(#[from] DbPutError),
    #[error("Failed to decode block hash hex: `{block_hash_hex}`")]
    DecodeBlockHashHex {
        block_hash_hex: String,
        source: hex::FromHexError,
    },
    #[error("Failed to get block `{block_hash}`")]
    GetBlock { block_hash: String },
    #[error("Failed to get block count")]
    GetBlockCount,
    #[error("Failed to get block hash for height `{height}`")]
    GetBlockHash { height: u32 },
    #[error("RPC error: `{method}`")]
    Rpc {
        method: String,
        source: ureq_jsonrpc::Error,
    },
    #[error(transparent)]
    WriteTxn(#[from] WriteTxnError),
}

#[derive(Clone)]
pub struct Bip300 {
    dbs: Dbs,
}

impl Bip300 {
    pub fn new(data_dir: &Path) -> Result<Self, CreateDbsError> {
        let dbs = Dbs::new(data_dir)?;
        Ok(Self { dbs })
    }

    // See https://github.com/LayerTwo-Labs/bip300_bip301_specifications/blob/master/bip300.md#m1-1
    fn handle_m1_propose_sidechain(
        &self,
        rwtxn: &mut RwTxn,
        proposal_height: u32,
        sidechain_number: u8,
        data: Vec<u8>,
    ) -> Result<(), HandleM1ProposeSidechainError> {
        let data_hash: Hash256 = sha256d(&data);
        if self
            .dbs
            .data_hash_to_sidechain_proposal
            .get(rwtxn, &data_hash)?
            .is_some()
        {
            // If a proposal with the same data_hash already exists,
            // we ignore this M1.
            //
            // Having the same data_hash means that data is the same as well.
            //
            // Without this rule it would be possible for the miners to reset the vote count for
            // any sidechain proposal at any point.
            return Ok(());
        }
        let sidechain_proposal = SidechainProposal {
            sidechain_number,
            data,
            vote_count: 0,
            proposal_height,
        };
        let () =
            self.dbs
                .data_hash_to_sidechain_proposal
                .put(rwtxn, &data_hash, &sidechain_proposal)?;
        Ok(())
    }

    // See https://github.com/LayerTwo-Labs/bip300_bip301_specifications/blob/master/bip300.md#m2-1
    fn handle_m2_ack_sidechain(
        &self,
        rwtxn: &mut RwTxn,
        height: u32,
        sidechain_number: u8,
        data_hash: [u8; 32],
    ) -> Result<(), HandleM2AckSidechainError> {
        let sidechain_proposal = self
            .dbs
            .data_hash_to_sidechain_proposal
            .get(rwtxn, &data_hash)?;
        let Some(mut sidechain_proposal) = sidechain_proposal else {
            return Ok(());
        };
        if sidechain_proposal.sidechain_number != sidechain_number {
            return Ok(());
        }
        sidechain_proposal.vote_count += 1;
        self.dbs
            .data_hash_to_sidechain_proposal
            .put(rwtxn, &data_hash, &sidechain_proposal)?;

        let sidechain_proposal_age = height - sidechain_proposal.proposal_height;

        let sidechain_slot_is_used = self
            .dbs
            .sidechain_number_to_sidechain
            .get(rwtxn, &sidechain_number)?
            .is_some();

        let new_sidechain_activated = {
            sidechain_slot_is_used
                && sidechain_proposal.vote_count > USED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD
                && sidechain_proposal_age <= USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
        } || {
            !sidechain_slot_is_used
                && sidechain_proposal.vote_count > UNUSED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD
                && sidechain_proposal_age <= UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
        };

        if new_sidechain_activated {
            println!(
                "sidechain {} in slot {sidechain_number} was activated",
                String::from_utf8_lossy(&sidechain_proposal.data),
            );
            let sidechain = Sidechain {
                sidechain_number,
                data: sidechain_proposal.data,
                proposal_height: sidechain_proposal.proposal_height,
                activation_height: height,
                vote_count: sidechain_proposal.vote_count,
            };
            self.dbs
                .sidechain_number_to_sidechain
                .put(rwtxn, &sidechain_number, &sidechain)?;
            self.dbs
                .data_hash_to_sidechain_proposal
                .delete(rwtxn, &data_hash)?;
        }
        Ok(())
    }

    fn handle_failed_sidechain_proposals(
        &self,
        rwtxn: &mut RwTxn,
        height: u32,
    ) -> Result<(), HandleFailedSidechainProposalsError> {
        let failed_proposals: Vec<_> = self
            .dbs
            .data_hash_to_sidechain_proposal
            .iter(rwtxn)
            .map_err(DbIterError::from)?
            .map_err(|err| HandleFailedSidechainProposalsError::DbIter(err.into()))
            .filter_map(|(data_hash, sidechain_proposal)| {
                let sidechain_proposal_age = height - sidechain_proposal.proposal_height;
                let sidechain_slot_is_used = self
                    .dbs
                    .sidechain_number_to_sidechain
                    .get(rwtxn, &sidechain_proposal.sidechain_number)?
                    .is_some();
                // FIXME: Do we need to check that the vote_count is below the threshold, or is it
                // enough to check that the max age was exceeded?
                let failed = sidechain_slot_is_used
                    && sidechain_proposal_age > USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
                    || !sidechain_slot_is_used
                        && sidechain_proposal_age > UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32;
                if failed {
                    Ok(Some(data_hash))
                } else {
                    Ok(None)
                }
            })
            .collect()?;
        for failed_proposal_data_hash in &failed_proposals {
            self.dbs
                .data_hash_to_sidechain_proposal
                .delete(rwtxn, failed_proposal_data_hash)?;
        }
        Ok(())
    }

    fn handle_m3_propose_bundle(
        &self,
        rwtxn: &mut RwTxn,
        sidechain_number: u8,
        m6id: [u8; 32],
    ) -> Result<(), HandleM3ProposeBundleError> {
        if self
            .dbs
            .sidechain_number_to_sidechain
            .get(rwtxn, &sidechain_number)?
            .is_none()
        {
            return Err(HandleM3ProposeBundleError::InactiveSidechain { sidechain_number });
        }
        let pending_m6ids = self
            .dbs
            .sidechain_number_to_pending_m6ids
            .get(rwtxn, &sidechain_number)?;
        let mut pending_m6ids = pending_m6ids.unwrap_or_default();
        let pending_m6id = PendingM6id {
            m6id,
            vote_count: 0,
        };
        pending_m6ids.push(pending_m6id);
        let () = self.dbs.sidechain_number_to_pending_m6ids.put(
            rwtxn,
            &sidechain_number,
            &pending_m6ids,
        )?;
        Ok(())
    }

    fn handle_m4_votes(
        &self,
        rwtxn: &mut RwTxn,
        upvotes: &[u16],
    ) -> Result<(), HandleM4VotesError> {
        for (sidechain_number, vote) in upvotes.iter().enumerate() {
            let vote = *vote;
            if vote == ABSTAIN_TWO_BYTES {
                continue;
            }
            let pending_m6ids = self
                .dbs
                .sidechain_number_to_pending_m6ids
                .get(rwtxn, &(sidechain_number as u8))?;
            let Some(mut pending_m6ids) = pending_m6ids else {
                continue;
            };
            if vote == ALARM_TWO_BYTES {
                for pending_m6id in &mut pending_m6ids {
                    if pending_m6id.vote_count > 0 {
                        pending_m6id.vote_count -= 1;
                    }
                }
            } else if let Some(pending_m6id) = pending_m6ids.get_mut(vote as usize) {
                pending_m6id.vote_count += 1;
            }
            let () = self.dbs.sidechain_number_to_pending_m6ids.put(
                rwtxn,
                &(sidechain_number as u8),
                &pending_m6ids,
            )?;
        }
        Ok(())
    }

    fn handle_m4_ack_bundles(
        &self,
        rwtxn: &mut RwTxn,
        m4: &M4AckBundles,
    ) -> Result<(), HandleM4AckBundlesError> {
        match m4 {
            M4AckBundles::LeadingBy50 => {
                todo!();
            }
            M4AckBundles::RepeatPrevious => {
                todo!();
            }
            M4AckBundles::OneByte { upvotes } => {
                let upvotes: Vec<u16> = upvotes.iter().map(|vote| *vote as u16).collect();
                self.handle_m4_votes(rwtxn, &upvotes)
                    .map_err(HandleM4AckBundlesError::from)
            }
            M4AckBundles::TwoBytes { upvotes } => self
                .handle_m4_votes(rwtxn, upvotes)
                .map_err(HandleM4AckBundlesError::from),
        }
    }

    fn handle_failed_m6ids(&self, rwtxn: &mut RwTxn) -> Result<(), HandleFailedM6IdsError> {
        let mut updated_slots = HashMap::new();
        let () = self
            .dbs
            .sidechain_number_to_pending_m6ids
            .iter(rwtxn)
            .map_err(DbIterError::from)?
            .map_err(DbIterError::from)
            .for_each(|(sidechain_number, pending_m6ids)| {
                let mut failed_m6ids = HashSet::new();
                for pending_m6id in &pending_m6ids {
                    if pending_m6id.vote_count > WITHDRAWAL_BUNDLE_MAX_AGE {
                        failed_m6ids.insert(pending_m6id.m6id);
                    }
                }
                let pending_m6ids: Vec<_> = pending_m6ids
                    .into_iter()
                    .filter(|pending_m6id| !failed_m6ids.contains(&pending_m6id.m6id))
                    .collect();
                updated_slots.insert(sidechain_number, pending_m6ids);
                Ok(())
            })?;
        for (sidechain_number, pending_m6ids) in updated_slots {
            let () = self.dbs.sidechain_number_to_pending_m6ids.put(
                rwtxn,
                &sidechain_number,
                &pending_m6ids,
            )?;
        }
        Ok(())
    }

    fn handle_m5_m6(
        &self,
        rwtxn: &mut RwTxn,
        transaction: &Transaction,
    ) -> Result<(), HandleM5M6Error> {
        // TODO: Check that there is only one OP_DRIVECHAIN per sidechain slot.
        let (sidechain_number, new_ctip, new_total_value) = {
            let output = &transaction.output[0];
            // If OP_DRIVECHAIN script is invalid,
            // for example if it is missing OP_TRUE at the end,
            // it will just be ignored.
            if let Ok((_input, sidechain_number)) =
                parse_op_drivechain(&output.script_pubkey.to_bytes())
            {
                let new_ctip = OutPoint {
                    txid: transaction.txid(),
                    vout: 0,
                };
                let new_total_value = output.value;

                (sidechain_number, new_ctip, new_total_value)
            } else {
                return Ok(());
            }
        };

        let address = {
            let output = &transaction.output[1];
            let script = output.script_pubkey.to_bytes();
            if script[0] == OP_RETURN.to_u8() {
                Some(script[1..].to_vec())
            } else {
                None
            }
        };

        let old_total_value = {
            if let Some(old_ctip) = self
                .dbs
                .sidechain_number_to_ctip
                .get(rwtxn, &sidechain_number)?
            {
                let old_ctip_found = transaction
                    .input
                    .iter()
                    .any(|input| input.previous_output == old_ctip.outpoint);
                if !old_ctip_found {
                    return Err(HandleM5M6Error::OldCtipUnspent { sidechain_number });
                }
                old_ctip.value
            } else {
                0
            }
        };
        let treasury_utxo = TreasuryUtxo {
            outpoint: new_ctip,
            address,
            total_value: new_total_value,
            previous_total_value: old_total_value,
        };
        dbg!(&treasury_utxo);

        // M6
        if new_total_value < old_total_value {
            let mut m6_valid = false;
            let m6id = m6_to_id(transaction, old_total_value);
            if let Some(pending_m6ids) = self
                .dbs
                .sidechain_number_to_pending_m6ids
                .get(rwtxn, &sidechain_number)?
            {
                for pending_m6id in &pending_m6ids {
                    if pending_m6id.m6id == m6id
                        && pending_m6id.vote_count > WITHDRAWAL_BUNDLE_INCLUSION_THRESHOLD
                    {
                        m6_valid = true;
                    }
                }
                if m6_valid {
                    let pending_m6ids: Vec<_> = pending_m6ids
                        .into_iter()
                        .filter(|pending_m6id| pending_m6id.m6id != m6id)
                        .collect();
                    self.dbs.sidechain_number_to_pending_m6ids.put(
                        rwtxn,
                        &sidechain_number,
                        &pending_m6ids,
                    )?;
                }
            }
            if !m6_valid {
                return Err(HandleM5M6Error::InvalidM6);
            }
        }
        let mut treasury_utxo_count = self
            .dbs
            .sidechain_number_to_treasury_utxo_count
            .get(rwtxn, &sidechain_number)?
            .unwrap_or(0);
        // Sequence numbers begin at 0, so the total number of treasury utxos in the database
        // gives us the *next* sequence number.
        let sequence_number = treasury_utxo_count;
        self.dbs
            .sidechain_number_sequence_number_to_treasury_utxo
            .put(rwtxn, &(sidechain_number, sequence_number), &treasury_utxo)?;
        treasury_utxo_count += 1;
        self.dbs.sidechain_number_to_treasury_utxo_count.put(
            rwtxn,
            &sidechain_number,
            &treasury_utxo_count,
        )?;
        let new_ctip = Ctip {
            outpoint: new_ctip,
            value: new_total_value,
        };
        self.dbs
            .sidechain_number_to_ctip
            .put(rwtxn, &sidechain_number, &new_ctip)?;
        Ok(())
    }

    fn handle_m8(
        transaction: &Transaction,
        accepted_bmm_requests: &HashSet<(u8, [u8; 32])>,
        prev_mainchain_block_hash: &[u8; 32],
    ) -> Result<(), HandleM8Error> {
        let output = &transaction.output[0];
        let script = output.script_pubkey.to_bytes();

        if let Ok((_input, bmm_request)) = parse_m8_bmm_request(&script) {
            if !accepted_bmm_requests.contains(&(
                bmm_request.sidechain_number,
                bmm_request.sidechain_block_hash,
            )) {
                return Err(HandleM8Error::NotAcceptedByMiners);
            }
            if bmm_request.prev_mainchain_block_hash != *prev_mainchain_block_hash {
                return Err(HandleM8Error::BmmRequestExpired);
            }
        }
        Ok(())
    }

    pub fn connect_block(
        &self,
        rwtxn: &mut RwTxn,
        block: &Block,
        height: u32,
    ) -> Result<(), ConnectBlockError> {
        // TODO: Check that there are no duplicate M2s.
        let coinbase = &block.txdata[0];
        let mut bmmed_sidechain_slots = HashSet::new();
        let mut accepted_bmm_requests = HashSet::new();
        for output in &coinbase.output {
            let Ok((_, message)) = parse_coinbase_script(&output.script_pubkey) else {
                continue;
            };
            match message {
                CoinbaseMessage::M1ProposeSidechain {
                    sidechain_number,
                    data,
                } => {
                    /*
                    println!(
                        "Propose sidechain number {sidechain_number} with data \"{}\"",
                        String::from_utf8(data.clone()).into_diagnostic()?,
                    );
                    */
                    self.handle_m1_propose_sidechain(
                        rwtxn,
                        height,
                        sidechain_number,
                        data.clone(),
                    )?;
                }
                CoinbaseMessage::M2AckSidechain {
                    sidechain_number,
                    data_hash,
                } => {
                    /*
                    println!(
                        "Ack sidechain number {sidechain_number} with hash {}",
                        hex::encode(data_hash)
                    );
                    */
                    self.handle_m2_ack_sidechain(rwtxn, height, sidechain_number, data_hash)?;
                }
                CoinbaseMessage::M3ProposeBundle {
                    sidechain_number,
                    bundle_txid,
                } => {
                    self.handle_m3_propose_bundle(rwtxn, sidechain_number, bundle_txid)?;
                }
                CoinbaseMessage::M4AckBundles(m4) => {
                    self.handle_m4_ack_bundles(rwtxn, &m4)?;
                }
                CoinbaseMessage::M7BmmAccept {
                    sidechain_number,
                    sidechain_block_hash,
                } => {
                    if bmmed_sidechain_slots.contains(&sidechain_number) {
                        return Err(ConnectBlockError::MultipleBmmBlocks { sidechain_number });
                    }
                    bmmed_sidechain_slots.insert(sidechain_number);
                    accepted_bmm_requests.insert((sidechain_number, sidechain_block_hash));
                }
            }
        }

        {
            let accepted_bmm_block_hashes: Vec<_> = accepted_bmm_requests
                .iter()
                .map(|(_sidechain_number, hash)| *hash)
                .collect();
            self.dbs.block_height_to_accepted_bmm_block_hashes.put(
                rwtxn,
                &height,
                &accepted_bmm_block_hashes,
            )?;
            const MAX_BMM_BLOCK_DEPTH: u64 = 6 * 24 * 7; // 1008 blocks = ~1 week of time
            if self
                .dbs
                .block_height_to_accepted_bmm_block_hashes
                .len(rwtxn)?
                > MAX_BMM_BLOCK_DEPTH
            {
                let (block_height, _) = self
                    .dbs
                    .block_height_to_accepted_bmm_block_hashes
                    .first(rwtxn)?
                    .unwrap();
                self.dbs
                    .block_height_to_accepted_bmm_block_hashes
                    .delete(rwtxn, &block_height)?;
            }
        }

        self.handle_failed_sidechain_proposals(rwtxn, height)?;
        self.handle_failed_m6ids(rwtxn)?;

        let prev_mainchain_block_hash = block.header.prev_blockhash.as_byte_array();

        for transaction in &block.txdata[1..] {
            self.handle_m5_m6(rwtxn, transaction)?;
            Self::handle_m8(
                transaction,
                &accepted_bmm_requests,
                prev_mainchain_block_hash,
            )?;
        }
        Ok(())
    }

    // TODO: Add unit tests ensuring that `connect_block` and `disconnect_block` are inverse
    // operations.
    pub fn _disconnect_block(&self, _block: &Block) -> Result<(), DisconnectBlockError> {
        todo!();
    }

    pub fn _is_transaction_valid(
        &self,
        _transaction: &Transaction,
    ) -> Result<(), TxValidationError> {
        todo!();
    }

    fn initial_sync(&self, main_client: &Client) -> Result<(), InitialSyncError> {
        let mut rwtxn = self.dbs.write_txn()?;
        let mut height = self
            .dbs
            .current_block_height
            .get(&rwtxn, &UnitKey)?
            .unwrap_or(0);
        let main_block_height: u32 = main_client
            .send_request("getblockcount", &[])
            .map_err(|err| InitialSyncError::Rpc {
                method: "getblockcount".to_owned(),
                source: err,
            })?
            .ok_or(InitialSyncError::GetBlockCount)?;
        while height < main_block_height {
            let block_hash: String = main_client
                .send_request("getblockhash", &[json!(height)])
                .map_err(|err| InitialSyncError::Rpc {
                    method: "getblockhash".to_owned(),
                    source: err,
                })?
                .ok_or(InitialSyncError::GetBlockHash { height })?;
            let block: String = main_client
                .send_request("getblock", &[json!(block_hash), json!(0)])
                .map_err(|err| InitialSyncError::Rpc {
                    method: "getblock".to_owned(),
                    source: err,
                })?
                .ok_or_else(|| InitialSyncError::GetBlock {
                    block_hash: block_hash.clone(),
                })?;
            let block_bytes = hex::decode(&block).unwrap();
            let mut cursor = Cursor::new(block_bytes);
            let block = Block::consensus_decode(&mut cursor).unwrap();
            self.connect_block(&mut rwtxn, &block, height)?;
            {
                /*
                main_client
                    .send_request("invalidateblock", &[json!(block_hash)])
                    .into_diagnostic()?
                    .ok_or(miette!("failed to invalidate block"))?;
                */
            }
            height += 1;
            let block_hash = {
                match hex::decode(&block_hash) {
                    Ok(block_hash) => block_hash,
                    Err(err) => {
                        return Err(InitialSyncError::DecodeBlockHashHex {
                            block_hash_hex: block_hash,
                            source: err,
                        })
                    }
                }
                .try_into()
                .unwrap()
            };
            self.dbs
                .current_chain_tip
                .put(&mut rwtxn, &UnitKey, &block_hash)?;
        }
        self.dbs
            .current_block_height
            .put(&mut rwtxn, &UnitKey, &height)?;
        let () = rwtxn.commit()?;
        Ok(())
    }

    // FIXME: Rewrite all of this to be more readable.
    /// Single iteration of the task loop
    fn task_loop_once(&self, main_client: &Client) -> Result<(), miette::Report> {
        let mut txn = self.dbs.write_txn().into_diagnostic()?;
        let mut height = self
            .dbs
            .current_block_height
            .get(&txn, &UnitKey)
            .into_diagnostic()?
            .unwrap_or(0);
        let main_block_height: u32 = main_client
            .send_request("getblockcount", &[])
            .into_diagnostic()?
            .ok_or(miette!("failed to get block count"))?;
        if main_block_height == height {
            return Ok(());
        }
        println!("Block height: {main_block_height}");

        while height < main_block_height {
            let block_hash: String = main_client
                .send_request("getblockhash", &[json!(height)])
                .into_diagnostic()?
                .ok_or(miette!("failed to get block hash"))?;
            let prev_blockhash = BlockHash::from_str(&block_hash).unwrap();

            println!("Mainchain tip: {prev_blockhash}");

            let block: String = main_client
                .send_request("getblock", &[json!(block_hash), json!(0)])
                .into_diagnostic()?
                .ok_or(miette!("failed to get block"))?;
            let block_bytes = hex::decode(&block).unwrap();
            let mut cursor = Cursor::new(block_bytes);
            let block = Block::consensus_decode(&mut cursor).unwrap();

            if self.connect_block(&mut txn, &block, height).is_err() {
                /*
                main_client
                    .send_request("invalidateblock", &[json!(block_hash)])
                    .into_diagnostic()?
                    .ok_or(miette!("failed to invalidate block"))?;
                */
            }
            println!();

            // check for new block
            // validate block
            // if invalid invalidate
            // if valid connect
            // wait 1 second
            height += 1;
            let block_hash = hex::decode(block_hash)
                .into_diagnostic()?
                .try_into()
                .unwrap();
            self.dbs
                .current_chain_tip
                .put(&mut txn, &UnitKey, &block_hash)
                .into_diagnostic()?;
        }
        self.dbs
            .current_block_height
            .put(&mut txn, &UnitKey, &height)
            .into_diagnostic()?;
        txn.commit().into_diagnostic()?;
        Ok(())
    }

    async fn task(&self, conf: Config) -> Result<(), miette::Report> {
        let main_client = &create_client(conf)?;
        let () = self.initial_sync(main_client).into_diagnostic()?;
        let interval = interval(Duration::from_secs(1));
        IntervalStream::new(interval)
            .map(Ok)
            .try_for_each(move |_: Instant| async move { self.task_loop_once(main_client) })
            .await
    }

    pub fn run(&self, conf: Config) -> JoinHandle<()> {
        let this = self.clone();
        tokio::task::spawn(async move {
            this.task(conf)
                .unwrap_or_else(|err| eprintln!("{err:#}"))
                .await
        })
    }

    pub fn get_sidechain_proposals(
        &self,
    ) -> Result<Vec<(Hash256, SidechainProposal)>, miette::Report> {
        let rotxn = self.dbs.read_txn().into_diagnostic()?;
        let res = self
            .dbs
            .data_hash_to_sidechain_proposal
            .iter(&rotxn)
            .into_diagnostic()?
            .collect()
            .into_diagnostic()?;
        Ok(res)
    }

    pub fn get_sidechains(&self) -> Result<Vec<Sidechain>, miette::Report> {
        let rotxn = self.dbs.read_txn().into_diagnostic()?;
        let res = self
            .dbs
            .sidechain_number_to_sidechain
            .iter(&rotxn)
            .into_diagnostic()?
            .map(|(_sidechain_number, sidechain)| Ok(sidechain))
            .collect()
            .into_diagnostic()?;
        Ok(res)
    }

    pub fn get_ctip_sequence_number(
        &self,
        sidechain_number: u8,
    ) -> Result<Option<u64>, miette::Report> {
        let rotxn = self.dbs.read_txn().into_diagnostic()?;
        let treasury_utxo_count = self
            .dbs
            .sidechain_number_to_treasury_utxo_count
            .get(&rotxn, &sidechain_number)
            .into_diagnostic()?;
        // Sequence numbers begin at 0, so the total number of treasury utxos in the database
        // gives us the *next* sequence number.
        // In order to get the current sequence number we decrement it by one.
        let sequence_number =
            treasury_utxo_count.map(|treasury_utxo_count| treasury_utxo_count - 1);
        Ok(sequence_number)
    }

    pub fn get_ctip(&self, sidechain_number: u8) -> Result<Option<Ctip>, miette::Report> {
        let txn = self.dbs.read_txn().into_diagnostic()?;
        let ctip = self
            .dbs
            .sidechain_number_to_ctip
            .get(&txn, &sidechain_number)
            .into_diagnostic()?;
        Ok(ctip)
    }

    /*
    pub fn get_main_block_height(&self) -> Result<u32> {
        let txn = self.env.read_txn().into_diagnostic()?;
        let height = self
            .current_block_height
            .get(&txn, &UnitKey)
            .into_diagnostic()?
            .unwrap_or(0);
        Ok(height)
    }

    pub fn get_main_chain_tip(&self) -> Result<[u8; 32]> {
        let txn = self.env.read_txn().into_diagnostic()?;
        let block_hash = self
            .current_chain_tip
            .get(&txn, &UnitKey)
            .into_diagnostic()?
            .unwrap_or([0; 32]);
        Ok(block_hash)
    }

    pub fn get_deposits(&self, sidechain_number: u8) -> Result<Vec<Deposit>> {
        let txn = self.env.read_txn().into_diagnostic()?;
        let treasury_utxos_range = self
            .sidechain_number_sequence_number_to_treasury_utxo
            .range(&txn, &((sidechain_number, 0)..(sidechain_number, u64::MAX)))
            .into_diagnostic()?;
        let mut deposits = vec![];
        for item in treasury_utxos_range {
            let ((_, sequence_number), treasury_utxo) = item.into_diagnostic()?;
            if treasury_utxo.total_value > treasury_utxo.previous_total_value
                && treasury_utxo.address.is_some()
            {
                let deposit = Deposit {
                    sequence_number,
                    address: treasury_utxo.address.unwrap(),
                    value: treasury_utxo.total_value - treasury_utxo.previous_total_value,
                };
                deposits.push(deposit);
            }
        }
        Ok(deposits)
    }
    */

    /*
    pub fn get_accepted_bmm_hashes(&self) -> Result<Vec<(u32, Vec<[u8; 32]>)>> {
        let mut block_height_accepted_bmm_hashes = vec![];
        let txn = self.env.read_txn().into_diagnostic()?;
        for item in self
            .block_height_to_accepted_bmm_block_hashes
            .iter(&txn)
            .into_diagnostic()?
        {
            let (block_height, accepted_bmm_hashes) = item.into_diagnostic()?;
            block_height_accepted_bmm_hashes.push((block_height, accepted_bmm_hashes.to_vec()));
        }
        Ok(block_height_accepted_bmm_hashes)
    }
    */
}

fn create_client(conf: Config) -> Result<Client, miette::Report> {
    if conf.node_rpc_user.is_none() != conf.node_rpc_password.is_none() {
        return Err(miette!("RPC user and password must be set together"));
    }

    if conf.node_rpc_user.is_none() == conf.node_rpc_cookie_path.is_none() {
        return Err(miette!("precisely one of RPC user and cookie must be set"));
    }

    let mut conf_user = conf.node_rpc_user.clone().unwrap_or_default();
    let mut conf_password = conf.node_rpc_password.clone().unwrap_or_default();

    if conf.node_rpc_cookie_path.is_some() {
        let cookie_path = conf.node_rpc_cookie_path.clone().unwrap();
        let auth = std::fs::read_to_string(cookie_path.clone())
            .map_err(|err| miette!("unable to read bitcoind cookie at {}: {}", cookie_path, err))?;

        let mut auth = auth.split(':');

        conf_user = auth
            .next()
            .ok_or(miette!("failed to get rpcuser"))?
            .to_string()
            .clone();

        conf_password = auth
            .next()
            .ok_or(miette!("failed to get rpcpassword"))?
            .to_string()
            .to_string()
            .clone();
    }

    Ok(Client {
        host: conf.node_rpc_host.to_string(),
        port: conf.node_rpc_port,
        user: conf_user.to_string(),
        password: conf_password.to_string(),
        id: "mainchain".into(),
    })
}
