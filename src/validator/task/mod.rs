use std::collections::HashSet;

use crate::{
    messages::{
        m6_to_id, parse_coinbase_script, parse_m8_bmm_request, parse_op_drivechain,
        CoinbaseMessage, M4AckBundles, ABSTAIN_TWO_BYTES, ALARM_TWO_BYTES,
    },
    types::SidechainProposalStatus,
};
use async_broadcast::{Sender, TrySendError};
use bip300301::{
    client::{GetBlockClient, U8Witness},
    jsonrpsee, MainClient,
};
use bitcoin::{
    self,
    hashes::{sha256d, Hash as _},
    opcodes::all::OP_RETURN,
    Amount, Block, BlockHash, OutPoint, Transaction, TxOut, Work,
};
use either::Either;
use fallible_iterator::FallibleIterator;
use fatality::Split as _;
use futures::{TryFutureExt as _, TryStreamExt as _};
use hashlink::{LinkedHashMap, LinkedHashSet};
use heed::RoTxn;

use crate::{
    types::{
        BlockInfo, BmmCommitments, Ctip, Deposit, Event, HeaderInfo, PendingM6id, Sidechain,
        SidechainNumber, SidechainProposal, TreasuryUtxo, WithdrawalBundleEvent,
        WithdrawalBundleEventKind,
    },
    validator::dbs::{db_error, Dbs, RwTxn, UnitKey},
    zmq::SequenceMessage,
};

mod error;

const WITHDRAWAL_BUNDLE_MAX_AGE: u16 = 10;
const WITHDRAWAL_BUNDLE_INCLUSION_THRESHOLD: u16 = WITHDRAWAL_BUNDLE_MAX_AGE / 2; // 5

const USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = WITHDRAWAL_BUNDLE_MAX_AGE; // 5
const USED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 = USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE / 2;

const UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE: u16 = 10;
const UNUSED_SIDECHAIN_SLOT_ACTIVATION_MAX_FAILS: u16 = 5;
const UNUSED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD: u16 =
    UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE - UNUSED_SIDECHAIN_SLOT_ACTIVATION_MAX_FAILS;

/// Returns `Some` if the sidechain proposal does not already exist
// See https://github.com/LayerTwo-Labs/bip300_bip301_specifications/blob/master/bip300.md#m1-1
fn handle_m1_propose_sidechain(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    proposal: SidechainProposal,
    proposal_height: u32,
) -> Result<Option<Sidechain>, error::HandleM1ProposeSidechain> {
    let description_hash: sha256d::Hash = proposal.description.sha256d_hash();
    // FIXME: check that the proposal was made in an ancestor block
    if dbs
        .description_hash_to_sidechain
        .contains_key(rwtxn, &description_hash)?
    {
        // If a proposal with the same description_hash already exists,
        // we ignore this M1.
        //
        // Having the same description_hash means that data is the same as well.
        //
        // Without this rule it would be possible for the miners to reset the vote count for
        // any sidechain proposal at any point.
        tracing::debug!("sidechain proposal already exists");
        return Ok(None);
    }
    let sidechain = Sidechain {
        proposal,
        status: SidechainProposalStatus {
            vote_count: 0,
            proposal_height,
            activation_height: None,
        },
    };

    let () = dbs
        .description_hash_to_sidechain
        .put(rwtxn, &description_hash, &sidechain)?;

    tracing::info!("persisted new sidechain proposal: {}", sidechain.proposal);
    Ok(Some(sidechain))
}

// See https://github.com/LayerTwo-Labs/bip300_bip301_specifications/blob/master/bip300.md#m2-1
fn handle_m2_ack_sidechain(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    height: u32,
    sidechain_number: SidechainNumber,
    description_hash: &sha256d::Hash,
) -> Result<(), error::HandleM2AckSidechain> {
    let sidechain = dbs
        .description_hash_to_sidechain
        .try_get(rwtxn, description_hash)?;
    let Some(mut sidechain) = sidechain else {
        return Ok(());
    };
    if sidechain.proposal.sidechain_number != sidechain_number {
        return Ok(());
    }
    sidechain.status.vote_count += 1;
    dbs.description_hash_to_sidechain
        .put(rwtxn, description_hash, &sidechain)?;

    let sidechain_proposal_age = height - sidechain.status.proposal_height;

    let sidechain_slot_is_used = dbs
        .active_sidechains
        .sidechain
        .try_get(rwtxn, &sidechain_number)?
        .is_some();

    let new_sidechain_activated = {
        sidechain_slot_is_used
            && sidechain.status.vote_count > USED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD
            && sidechain_proposal_age <= USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
    } || {
        !sidechain_slot_is_used
            && sidechain.status.vote_count > UNUSED_SIDECHAIN_SLOT_ACTIVATION_THRESHOLD
            && sidechain_proposal_age <= UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
    };

    if new_sidechain_activated {
        tracing::info!(
            "sidechain {} in slot {} was activated",
            String::from_utf8_lossy(&sidechain.proposal.description.0),
            sidechain_number.0
        );
        sidechain.status.activation_height = Some(height);
        dbs.active_sidechains
            .sidechain
            .put(rwtxn, &sidechain_number, &sidechain)?;
        dbs.description_hash_to_sidechain
            .delete(rwtxn, description_hash)?;
    }
    Ok(())
}

fn handle_failed_sidechain_proposals(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    height: u32,
) -> Result<(), error::HandleFailedSidechainProposals> {
    let failed_proposals: Vec<_> = dbs
        .description_hash_to_sidechain
        .iter(rwtxn)
        .map_err(db_error::Iter::from)?
        .map_err(|err| error::HandleFailedSidechainProposals::DbIter(err.into()))
        .filter_map(|(description_hash, sidechain)| {
            let sidechain_proposal_age = height - sidechain.status.proposal_height;
            let sidechain_slot_is_used = dbs
                .active_sidechains
                .sidechain
                .try_get(rwtxn, &sidechain.proposal.sidechain_number)?
                .is_some();
            // FIXME: Do we need to check that the vote_count is below the threshold, or is it
            // enough to check that the max age was exceeded?
            let failed = sidechain_slot_is_used
                && sidechain_proposal_age > USED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32
                || !sidechain_slot_is_used
                    && sidechain_proposal_age > UNUSED_SIDECHAIN_SLOT_PROPOSAL_MAX_AGE as u32;
            if failed {
                Ok(Some(description_hash))
            } else {
                Ok(None)
            }
        })
        .collect()?;
    for failed_description_hash in &failed_proposals {
        dbs.description_hash_to_sidechain
            .delete(rwtxn, failed_description_hash)?;
    }
    Ok(())
}

fn handle_m3_propose_bundle(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    sidechain_number: SidechainNumber,
    m6id: [u8; 32],
) -> Result<(), error::HandleM3ProposeBundle> {
    if !dbs
        .active_sidechains
        .sidechain
        .contains_key(rwtxn, &sidechain_number)?
    {
        return Err(error::HandleM3ProposeBundle::InactiveSidechain { sidechain_number });
    }
    let pending_m6ids = dbs
        .active_sidechains
        .pending_m6ids
        .try_get(rwtxn, &sidechain_number)?;
    let mut pending_m6ids = pending_m6ids.unwrap_or_default();
    let pending_m6id = PendingM6id {
        m6id,
        vote_count: 0,
    };
    pending_m6ids.push(pending_m6id);
    let () = dbs
        .active_sidechains
        .pending_m6ids
        .put(rwtxn, &sidechain_number, &pending_m6ids)?;
    Ok(())
}

fn handle_m4_votes(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    upvotes: &[u16],
) -> Result<(), error::HandleM4Votes> {
    for (sidechain_number, vote) in upvotes.iter().enumerate() {
        let sidechain_number = (sidechain_number as u8).into();
        let vote = *vote;
        if vote == ABSTAIN_TWO_BYTES {
            continue;
        }
        let pending_m6ids = dbs
            .active_sidechains
            .pending_m6ids
            .try_get(rwtxn, &sidechain_number)?;
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
        let () =
            dbs.active_sidechains
                .pending_m6ids
                .put(rwtxn, &sidechain_number, &pending_m6ids)?;
    }
    Ok(())
}

fn handle_m4_ack_bundles(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    m4: &M4AckBundles,
) -> Result<(), error::HandleM4AckBundles> {
    match m4 {
        M4AckBundles::LeadingBy50 => {
            todo!();
        }
        M4AckBundles::RepeatPrevious => {
            todo!();
        }
        M4AckBundles::OneByte { upvotes } => {
            let upvotes: Vec<u16> = upvotes.iter().map(|vote| *vote as u16).collect();
            handle_m4_votes(rwtxn, dbs, &upvotes).map_err(error::HandleM4AckBundles::from)
        }
        M4AckBundles::TwoBytes { upvotes } => {
            handle_m4_votes(rwtxn, dbs, upvotes).map_err(error::HandleM4AckBundles::from)
        }
    }
}

/// Returns failed M6IDs with sidechain numbers
fn handle_failed_m6ids(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
) -> Result<LinkedHashSet<(SidechainNumber, [u8; 32])>, error::HandleFailedM6Ids> {
    let mut failed_m6ids = LinkedHashSet::new();
    let mut updated_slots = LinkedHashMap::new();
    let () = dbs
        .active_sidechains
        .pending_m6ids
        .iter(rwtxn)
        .map_err(db_error::Iter::from)?
        .map_err(db_error::Iter::from)
        .for_each(|(sidechain_number, pending_m6ids)| {
            for pending_m6id in &pending_m6ids {
                if pending_m6id.vote_count > WITHDRAWAL_BUNDLE_MAX_AGE {
                    failed_m6ids.insert((sidechain_number, pending_m6id.m6id));
                }
            }
            let pending_m6ids: Vec<_> = pending_m6ids
                .into_iter()
                .filter(|pending_m6id| {
                    !failed_m6ids.contains(&(sidechain_number, pending_m6id.m6id))
                })
                .collect();
            updated_slots.insert(sidechain_number, pending_m6ids);
            Ok(())
        })?;
    for (sidechain_number, pending_m6ids) in updated_slots {
        let () =
            dbs.active_sidechains
                .pending_m6ids
                .put(rwtxn, &sidechain_number, &pending_m6ids)?;
    }
    Ok(failed_m6ids)
}

/// Deposit or (sidechain_id, m6id)
type DepositOrSuccessfulWithdrawal = Either<Deposit, (SidechainNumber, [u8; 32])>;

fn handle_m5_m6(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    transaction: &Transaction,
) -> Result<Option<DepositOrSuccessfulWithdrawal>, error::HandleM5M6> {
    let txid = transaction.compute_txid();
    // TODO: Check that there is only one OP_DRIVECHAIN per sidechain slot.
    let (sidechain_number, new_ctip, new_total_value) = {
        let output = &transaction.output[0];
        // If OP_DRIVECHAIN script is invalid,
        // for example if it is missing OP_TRUE at the end,
        // it will just be ignored.
        if let Ok((_input, sidechain_number)) =
            parse_op_drivechain(&output.script_pubkey.to_bytes())
        {
            let new_ctip = OutPoint { txid, vout: 0 };
            let new_total_value = output.value;

            (sidechain_number, new_ctip, new_total_value)
        } else {
            return Ok(None);
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
        if let Some(old_ctip) = dbs
            .active_sidechains
            .ctip
            .try_get(rwtxn, &sidechain_number)?
        {
            let old_ctip_found = transaction
                .input
                .iter()
                .any(|input| input.previous_output == old_ctip.outpoint);
            if !old_ctip_found {
                return Err(error::HandleM5M6::OldCtipUnspent { sidechain_number });
            }
            old_ctip.value
        } else {
            Amount::ZERO
        }
    };
    let treasury_utxo = TreasuryUtxo {
        outpoint: new_ctip,
        address: address.clone(),
        total_value: new_total_value,
        previous_total_value: old_total_value,
    };
    dbg!(&treasury_utxo);

    let mut res = None;
    // M6
    if new_total_value < old_total_value {
        let mut m6_valid = false;
        let m6id = m6_to_id(transaction, old_total_value.to_sat());
        if let Some(pending_m6ids) = dbs
            .active_sidechains
            .pending_m6ids
            .try_get(rwtxn, &sidechain_number)?
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
                dbs.active_sidechains.pending_m6ids.put(
                    rwtxn,
                    &sidechain_number,
                    &pending_m6ids,
                )?;
            }
        }
        if m6_valid {
            res = Some(Either::Right((sidechain_number, m6id)));
        } else {
            return Err(error::HandleM5M6::InvalidM6);
        }
    }
    let mut treasury_utxo_count = dbs
        .active_sidechains
        .treasury_utxo_count
        .try_get(rwtxn, &sidechain_number)?
        .unwrap_or(0);
    // Sequence numbers begin at 0, so the total number of treasury utxos in the database
    // gives us the *next* sequence number.
    let sequence_number = treasury_utxo_count;
    dbs.active_sidechains.slot_sequence_to_treasury_utxo.put(
        rwtxn,
        &(sidechain_number, sequence_number),
        &treasury_utxo,
    )?;
    treasury_utxo_count += 1;
    dbs.active_sidechains.treasury_utxo_count.put(
        rwtxn,
        &sidechain_number,
        &treasury_utxo_count,
    )?;
    let new_ctip = Ctip {
        outpoint: new_ctip,
        value: new_total_value,
    };
    dbs.active_sidechains
        .ctip
        .put(rwtxn, &sidechain_number, &new_ctip)?;
    match address {
        Some(address) if new_total_value >= old_total_value && res.is_none() => {
            let deposit = Deposit {
                sequence_number,
                sidechain_id: sidechain_number,
                outpoint: OutPoint { txid, vout: 0 },
                output: TxOut {
                    value: new_total_value - old_total_value,
                    script_pubkey: address.into(),
                },
            };
            res = Some(Either::Left(deposit));
        }
        Some(_) | None => (),
    }
    Ok(res)
}

/// Handles a (potential) M8 BMM request.
/// Returns `true` if this is a valid BMM request, `HandleM8Error::Jfyi` if
/// this is an invalid BMM request, and `false` if this is not a BMM request.
fn handle_m8(
    transaction: &Transaction,
    accepted_bmm_requests: &BmmCommitments,
    prev_mainchain_block_hash: &BlockHash,
) -> Result<bool, error::HandleM8> {
    let output = &transaction.output[0];
    let script = output.script_pubkey.to_bytes();

    if let Ok((_input, bmm_request)) = parse_m8_bmm_request(&script) {
        if !accepted_bmm_requests
            .get(&bmm_request.sidechain_number)
            .is_some_and(|commitment| *commitment == bmm_request.sidechain_block_hash)
        {
            Err(error::HandleM8::NotAcceptedByMiners)
        } else if bmm_request.prev_mainchain_block_hash != prev_mainchain_block_hash.to_byte_array()
        {
            Err(error::HandleM8::BmmRequestExpired)
        } else {
            Ok(true)
        }
    } else {
        Ok(false)
    }
}

fn connect_block(
    rwtxn: &mut RwTxn,
    dbs: &Dbs,
    event_tx: &Sender<Event>,
    block: &Block,
    height: u32,
) -> Result<(), error::ConnectBlock> {
    // TODO: Check that there are no duplicate M2s.
    let coinbase = &block.txdata[0];
    let mut bmmed_sidechain_slots = HashSet::new();
    let mut accepted_bmm_requests = BmmCommitments::new();
    let mut sidechain_proposals = Vec::new();
    let mut withdrawal_bundle_events = Vec::new();
    for (vout, output) in coinbase.output.iter().enumerate() {
        let message = match parse_coinbase_script(&output.script_pubkey) {
            Ok((rest, message)) => {
                if !rest.is_empty() {
                    tracing::warn!("Extra data in coinbase script: {:?}", hex::encode(rest));
                }
                message
            }

            Err(err) => {
                // Happens all the time. Would be nice to differentiate between "this isn't a BIP300 message"
                // and "we failed real bad".
                tracing::trace!("Failed to parse coinbase script: {:?}", err);
                continue;
            }
        };

        match message {
            CoinbaseMessage::M1ProposeSidechain {
                sidechain_number,
                data,
            } => {
                tracing::info!(
                    "Propose sidechain number {sidechain_number} with data \"{}\"",
                    String::from_utf8_lossy(&data)
                );
                let sidechain_proposal = SidechainProposal {
                    sidechain_number,
                    description: data.into(),
                };
                if let Some(sidechain) =
                    handle_m1_propose_sidechain(rwtxn, dbs, sidechain_proposal, height)?
                {
                    // sidechain proposal is new
                    sidechain_proposals.push((vout as u32, sidechain.proposal));
                }
            }
            CoinbaseMessage::M2AckSidechain {
                sidechain_number,
                data_hash: description_hash,
            } => {
                tracing::info!(
                    "Ack sidechain number {sidechain_number} with proposal description hash {}",
                    hex::encode(description_hash)
                );
                handle_m2_ack_sidechain(
                    rwtxn,
                    dbs,
                    height,
                    sidechain_number,
                    &sha256d::Hash::from_byte_array(description_hash),
                )?;
            }
            CoinbaseMessage::M3ProposeBundle {
                sidechain_number,
                bundle_txid,
            } => {
                let () = handle_m3_propose_bundle(rwtxn, dbs, sidechain_number, bundle_txid)?;
                let event = WithdrawalBundleEvent {
                    sidechain_id: sidechain_number,
                    m6id: bundle_txid,
                    kind: WithdrawalBundleEventKind::Submitted,
                };
                withdrawal_bundle_events.push(event);
            }
            CoinbaseMessage::M4AckBundles(m4) => {
                handle_m4_ack_bundles(rwtxn, dbs, &m4)?;
            }
            CoinbaseMessage::M7BmmAccept {
                sidechain_number,
                sidechain_block_hash,
            } => {
                if bmmed_sidechain_slots.contains(&sidechain_number) {
                    return Err(error::ConnectBlock::MultipleBmmBlocks { sidechain_number });
                }
                bmmed_sidechain_slots.insert(sidechain_number);
                accepted_bmm_requests.insert(sidechain_number, sidechain_block_hash);
            }
        }
    }

    let () = handle_failed_sidechain_proposals(rwtxn, dbs, height)?;
    let failed_m6ids = handle_failed_m6ids(rwtxn, dbs)?;

    let block_hash = block.header.block_hash();
    let prev_mainchain_block_hash = block.header.prev_blockhash;

    let mut deposits = Vec::new();
    withdrawal_bundle_events.extend(failed_m6ids.into_iter().map(|(sidechain_id, m6id)| {
        WithdrawalBundleEvent {
            m6id,
            sidechain_id,
            kind: WithdrawalBundleEventKind::Failed,
        }
    }));
    for transaction in &block.txdata[1..] {
        match handle_m5_m6(rwtxn, dbs, transaction)? {
            Some(Either::Left(deposit)) => deposits.push(deposit),
            Some(Either::Right((sidechain_id, m6id))) => {
                let withdrawal_bundle_event = WithdrawalBundleEvent {
                    m6id,
                    sidechain_id,
                    kind: WithdrawalBundleEventKind::Succeeded,
                };
                withdrawal_bundle_events.push(withdrawal_bundle_event);
            }
            None => (),
        };
        if handle_m8(
            transaction,
            &accepted_bmm_requests,
            &prev_mainchain_block_hash,
        )? {
            tracing::trace!(
                "Handled valid M8 BMM request in tx `{}`",
                transaction.compute_txid()
            );
        }
    }
    let block_info = BlockInfo {
        bmm_commitments: accepted_bmm_requests.into_iter().collect(),
        coinbase_txid: coinbase.compute_txid(),
        deposits,
        sidechain_proposals,
        withdrawal_bundle_events,
    };
    let () = dbs
        .block_hashes
        .put_block_info(rwtxn, &block_hash, &block_info)
        .map_err(error::ConnectBlock::PutBlockInfo)?;
    // TODO: invalidate block
    let current_tip_cumulative_work: Option<Work> = 'work: {
        let Some(current_tip) = dbs.current_chain_tip.try_get(rwtxn, &UnitKey)? else {
            break 'work None;
        };
        Some(
            dbs.block_hashes
                .cumulative_work()
                .get(rwtxn, &current_tip)?,
        )
    };
    let cumulative_work = dbs.block_hashes.cumulative_work().get(rwtxn, &block_hash)?;
    if Some(cumulative_work) > current_tip_cumulative_work {
        dbs.current_chain_tip.put(rwtxn, &UnitKey, &block_hash)?;
        tracing::debug!("updated current chain tip to {block_hash}");
    }
    let event = {
        let header_info = HeaderInfo {
            block_hash,
            prev_block_hash: prev_mainchain_block_hash,
            height,
            work: block.header.work(),
        };
        Event::ConnectBlock {
            header_info,
            block_info,
        }
    };
    let _send_err: Result<Option<_>, TrySendError<_>> = event_tx.try_broadcast(event);
    Ok(())
}

// TODO: Add unit tests ensuring that `connect_block` and `disconnect_block` are inverse
// operations.
#[allow(unreachable_code, unused_variables)]
fn disconnect_block(
    _rwtxn: &mut RwTxn,
    _dbs: &Dbs,
    event_tx: &Sender<Event>,
    block_hash: BlockHash,
) -> Result<(), error::DisconnectBlock> {
    // FIXME: implement
    todo!();
    let event = Event::DisconnectBlock { block_hash };
    let _send_err: Result<Option<_>, TrySendError<_>> = event_tx.try_broadcast(event);
    Ok(())
}

fn _is_transaction_valid(
    _rotxn: &mut RoTxn,
    _dbs: &Dbs,
    _transaction: &Transaction,
) -> Result<(), error::TxValidation> {
    todo!();
}

async fn sync_headers(
    dbs: &Dbs,
    main_client: &jsonrpsee::http_client::HttpClient,
    main_tip: BlockHash,
) -> Result<(), error::Sync> {
    let mut block_hash = main_tip;
    while let Some((latest_missing_header, latest_missing_header_height)) =
        tokio::task::block_in_place(|| {
            let rotxn = dbs.read_txn()?;
            match dbs
                .block_hashes
                .latest_missing_ancestor_header(&rotxn, block_hash)
                .map_err(error::Sync::DbTryGet)?
            {
                Some(latest_missing_header) => {
                    let height = dbs
                        .block_hashes
                        .height()
                        .try_get(&rotxn, &latest_missing_header)?;
                    Ok::<_, error::Sync>(Some((latest_missing_header, height)))
                }
                None => Ok(None),
            }
        })?
    {
        if let Some(latest_missing_header_height) = latest_missing_header_height {
            tracing::debug!("Syncing header #{latest_missing_header_height} `{latest_missing_header}` -> `{main_tip}`");
        } else {
            tracing::debug!("Syncing header `{latest_missing_header}` -> `{main_tip}`");
        }
        let header = main_client
            .getblockheader(latest_missing_header)
            .map_err(|err| error::Sync::JsonRpc {
                method: "getblockheader".to_owned(),
                source: err,
            })
            .await?;
        latest_missing_header_height.inspect(|height| assert_eq!(*height, header.height));
        let height = header.height;
        let mut rwtxn = dbs.write_txn()?;
        dbs.block_hashes
            .put_header(&mut rwtxn, &header.into(), height)?;
        let () = rwtxn.commit()?;
        block_hash = latest_missing_header;
    }
    Ok(())
}

// MUST be called after `initial_sync_headers`.
async fn sync_blocks(
    dbs: &Dbs,
    event_tx: &Sender<Event>,
    main_client: &jsonrpsee::http_client::HttpClient,
    main_tip: BlockHash,
) -> Result<(), error::Sync> {
    let missing_blocks: Vec<BlockHash> = tokio::task::block_in_place(|| {
        let rotxn = dbs.read_txn()?;
        dbs.block_hashes
            .ancestor_headers(&rotxn, main_tip)
            .map(|(block_hash, _header)| Ok(block_hash))
            .take_while(|block_hash| Ok(!dbs.block_hashes.contains_block(&rotxn, block_hash)?))
            .collect()
            .map_err(error::Sync::from)
    })?;
    if missing_blocks.is_empty() {
        return Ok(());
    }
    for missing_block in missing_blocks.into_iter().rev() {
        tracing::debug!("Syncing block `{missing_block}` -> `{main_tip}`");
        let block = main_client
            .get_block(missing_block, U8Witness::<0>)
            .map_err(|err| error::Sync::JsonRpc {
                method: "getblock".to_owned(),
                source: err,
            })
            .await?
            .0;
        let mut rwtxn = dbs.write_txn()?;
        let height = dbs.block_hashes.height().get(&rwtxn, &missing_block)?;
        let () = connect_block(&mut rwtxn, dbs, event_tx, &block, height)?;
        tracing::debug!("connected block at height {height}: {missing_block}");
        let () = rwtxn.commit()?;
    }
    Ok(())
}

async fn sync_to_tip(
    dbs: &Dbs,
    event_tx: &Sender<Event>,
    main_client: &jsonrpsee::http_client::HttpClient,
    main_tip: BlockHash,
) -> Result<(), error::Sync> {
    let () = sync_headers(dbs, main_client, main_tip).await?;
    let () = sync_blocks(dbs, event_tx, main_client, main_tip).await?;
    Ok(())
}

async fn initial_sync(
    dbs: &Dbs,
    event_tx: &Sender<Event>,
    main_client: &jsonrpsee::http_client::HttpClient,
) -> Result<(), error::Sync> {
    let main_tip: BlockHash = main_client
        .getbestblockhash()
        .map_err(|err| error::Sync::JsonRpc {
            method: "getbestblockhash".to_owned(),
            source: err,
        })
        .await?;
    tracing::debug!("mainchain tip: `{main_tip}`");
    let () = sync_to_tip(dbs, event_tx, main_client, main_tip).await?;
    Ok(())
}

pub(super) async fn task(
    main_client: &jsonrpsee::http_client::HttpClient,
    zmq_addr_sequence: &str,
    dbs: &Dbs,
    event_tx: &Sender<Event>,
) -> Result<(), error::Fatal> {
    // FIXME: use this instead of polling
    let zmq_sequence = crate::zmq::subscribe_sequence(zmq_addr_sequence)
        .await
        .map_err(error::Fatal::from)?;
    let () = initial_sync(dbs, event_tx, main_client)
        .await
        .or_else(|err| {
            let non_fatal: <error::Sync as fatality::Split>::Jfyi = err.split()?;
            let non_fatal = anyhow::Error::from(non_fatal);
            tracing::warn!("Error during initial sync: {non_fatal:#}");
            Ok::<(), error::Fatal>(())
        })?;
    zmq_sequence
        .err_into::<error::Fatal>()
        .try_for_each(|msg| async move {
            match msg {
                SequenceMessage::BlockHashConnected(block_hash, _) => {
                    let () = sync_to_tip(dbs, event_tx, main_client, block_hash)
                        .await
                        .or_else(|err| {
                            let non_fatal: <error::Sync as fatality::Split>::Jfyi = err.split()?;
                            let non_fatal = anyhow::Error::from(non_fatal);
                            tracing::warn!("Error during sync to {block_hash}: {non_fatal:#}");
                            Ok::<(), error::Fatal>(())
                        })?;
                    Ok(())
                }
                SequenceMessage::BlockHashDisconnected(block_hash, _) => {
                    let mut rwtxn = dbs.write_txn()?;
                    let () = disconnect_block(&mut rwtxn, dbs, event_tx, block_hash)?;
                    Ok(())
                }
                SequenceMessage::TxHashAdded { .. } | SequenceMessage::TxHashRemoved { .. } => {
                    Ok(())
                }
            }
        })
        .await
        .map_err(error::Fatal::from)
}