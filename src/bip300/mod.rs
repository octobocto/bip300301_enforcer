use std::{future::Future, path::Path, sync::Arc};

use async_broadcast::{broadcast, InactiveReceiver, Receiver};
use bip300301_messages::bitcoin::BlockHash;
use fallible_iterator::FallibleIterator;
use futures::FutureExt;
use miette::IntoDiagnostic;
use thiserror::Error;
use tokio::task::{spawn, JoinHandle};

use crate::{
    cli::Config,
    types::{
        BlockInfo, BmmCommitments, Ctip, Event, Hash256, HeaderInfo, Sidechain, SidechainNumber,
        SidechainProposal, TwoWayPegData,
    },
};

mod dbs;
mod task;

use dbs::{CreateDbsError, Dbs};

#[derive(Debug, Error)]
pub enum GetBlockInfoError {
    #[error(transparent)]
    ReadTxn(#[from] dbs::ReadTxnError),
    #[error(transparent)]
    GetBlockInfo(#[from] dbs::GetBlockInfoError),
}

#[derive(Debug, Error)]
pub enum GetHeaderInfoError {
    #[error(transparent)]
    ReadTxn(#[from] dbs::ReadTxnError),
    #[error(transparent)]
    GetHeaderInfo(#[from] dbs::GetHeaderInfoError),
}

#[derive(Debug, Error)]
pub enum GetTwoWayPegDataError {
    #[error(transparent)]
    ReadTxn(#[from] dbs::ReadTxnError),
    #[error(transparent)]
    GetTwoWayPegData(#[from] dbs::GetTwoWayPegDataError),
}

#[derive(Debug, Error)]
pub enum TryGetBmmCommitmentsError {
    #[error(transparent)]
    ReadTxn(#[from] dbs::ReadTxnError),
    #[error(transparent)]
    DbTryGet(#[from] dbs::DbTryGetError),
}

#[derive(Clone)]
pub struct Bip300 {
    dbs: Dbs,
    events_rx: InactiveReceiver<Event>,
    task: Arc<JoinHandle<()>>,
}

impl Bip300 {
    pub fn new<F, Fut>(
        conf: Config,
        data_dir: &Path,
        err_handler: F,
    ) -> Result<Self, CreateDbsError>
    where
        F: FnOnce(miette::Report) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        const EVENTS_CHANNEL_CAPACITY: usize = 256;
        let dbs = Dbs::new(data_dir)?;
        // FIXME: pass events_tx to task
        let (events_tx, mut events_rx) = broadcast(EVENTS_CHANNEL_CAPACITY);
        events_rx.set_await_active(false);
        events_rx.set_overflow(true);
        let task = spawn({
            let dbs = dbs.clone();
            async move {
                task::task(conf, &dbs, &events_tx)
                    .then(|res| async {
                        if let Err(err) = res {
                            err_handler(err).await
                        }
                    })
                    .await
            }
        });
        Ok(Self {
            dbs,
            events_rx: events_rx.deactivate(),
            task: Arc::new(task),
        })
    }

    pub fn subscribe_events(&self) -> Receiver<Event> {
        self.events_rx.activate_cloned()
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
        sidechain_number: SidechainNumber,
    ) -> Result<Option<u64>, miette::Report> {
        let rotxn = self.dbs.read_txn().into_diagnostic()?;
        let treasury_utxo_count = self
            .dbs
            .sidechain_number_to_treasury_utxo_count
            .try_get(&rotxn, &sidechain_number)
            .into_diagnostic()?;
        // Sequence numbers begin at 0, so the total number of treasury utxos in the database
        // gives us the *next* sequence number.
        // In order to get the current sequence number we decrement it by one.
        let sequence_number =
            treasury_utxo_count.map(|treasury_utxo_count| treasury_utxo_count - 1);
        Ok(sequence_number)
    }

    pub fn get_ctip(
        &self,
        sidechain_number: SidechainNumber,
    ) -> Result<Option<Ctip>, miette::Report> {
        let txn = self.dbs.read_txn().into_diagnostic()?;
        let ctip = self
            .dbs
            .sidechain_number_to_ctip
            .try_get(&txn, &sidechain_number)
            .into_diagnostic()?;
        Ok(ctip)
    }

    pub fn get_block_info(&self, block_hash: &BlockHash) -> Result<BlockInfo, GetBlockInfoError> {
        let rotxn = self.dbs.read_txn()?;
        let res = self.dbs.get_block_info(&rotxn, &block_hash)?;
        Ok(res)
    }

    pub fn get_header_info(
        &self,
        block_hash: &BlockHash,
    ) -> Result<HeaderInfo, GetHeaderInfoError> {
        let rotxn = self.dbs.read_txn()?;
        let res = self.dbs.get_header_info(&rotxn, &block_hash)?;
        Ok(res)
    }

    pub fn get_mainchain_tip(&self) -> Result<BlockHash, miette::Report> {
        let txn = self.dbs.read_txn().into_diagnostic()?;
        self.dbs
            .current_chain_tip
            .get(&txn, &dbs::UnitKey)
            .into_diagnostic()
    }

    pub fn get_two_way_peg_data(
        &self,
        start_block: Option<BlockHash>,
        end_block: BlockHash,
    ) -> Result<Vec<TwoWayPegData>, GetTwoWayPegDataError> {
        let rotxn = self.dbs.read_txn()?;
        let res = self
            .dbs
            .get_two_way_peg_data(&rotxn, start_block, end_block)?;
        Ok(res)
    }

    pub fn try_get_bmm_commitments(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BmmCommitments>, TryGetBmmCommitmentsError> {
        let rotxn = self.dbs.read_txn()?;
        let res = self
            .dbs
            .block_hash_to_bmm_commitments
            .try_get(&rotxn, block_hash)?;
        Ok(res)
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

impl Drop for Bip300 {
    fn drop(&mut self) {
        self.task.abort()
    }
}
