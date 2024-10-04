use std::path::{Path, PathBuf};

use heed::{types::SerdeBincode, EnvOpenOptions, RoTxn};
use thiserror::Error;

use crate::types::{
    Ctip, Deposit, Hash256, PendingM6id, Sidechain, SidechainNumber, SidechainProposal,
    TreasuryUtxo,
};

mod util;

pub use util::{
    CommitWriteTxnError, Database, DbDeleteError, DbFirstError, DbGetError, DbIterError,
    DbLenError, DbPutError, Env, ReadTxnError, RwTxn, UnitKey, WriteTxnError,
};

#[derive(Debug, Error)]
pub enum CreateDbsError {
    #[error(transparent)]
    CommitWriteTxn(#[from] util::CommitWriteTxnError),
    #[error(transparent)]
    CreateDb(#[from] util::CreateDbError),
    #[error("Error creating directory (`{path}`)")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    OpenEnv(#[from] util::OpenEnvError),
    #[error(transparent)]
    WriteTxn(#[from] util::WriteTxnError),
}

#[derive(Clone)]
pub(super) struct Dbs {
    env: Env,
    pub block_hash_to_deposits: Database<SerdeBincode<Hash256>, SerdeBincode<Vec<Deposit>>>,
    pub block_height_to_accepted_bmm_block_hashes:
        Database<SerdeBincode<u32>, SerdeBincode<Vec<Hash256>>>,
    pub current_block_height: Database<SerdeBincode<UnitKey>, SerdeBincode<u32>>,
    pub current_chain_tip: Database<SerdeBincode<UnitKey>, SerdeBincode<Hash256>>,
    pub data_hash_to_sidechain_proposal:
        Database<SerdeBincode<Hash256>, SerdeBincode<SidechainProposal>>,
    pub _leading_by_50: Database<SerdeBincode<UnitKey>, SerdeBincode<Vec<Hash256>>>,
    pub _previous_votes: Database<SerdeBincode<UnitKey>, SerdeBincode<Vec<Hash256>>>,
    pub sidechain_number_sequence_number_to_treasury_utxo:
        Database<SerdeBincode<(SidechainNumber, u64)>, SerdeBincode<TreasuryUtxo>>,
    pub sidechain_number_to_ctip: Database<SerdeBincode<SidechainNumber>, SerdeBincode<Ctip>>,
    pub sidechain_number_to_pending_m6ids:
        Database<SerdeBincode<SidechainNumber>, SerdeBincode<Vec<PendingM6id>>>,
    pub sidechain_number_to_sidechain:
        Database<SerdeBincode<SidechainNumber>, SerdeBincode<Sidechain>>,
    pub sidechain_number_to_treasury_utxo_count:
        Database<SerdeBincode<SidechainNumber>, SerdeBincode<u64>>,
}

impl Dbs {
    const NUM_DBS: u32 = 12;

    pub fn new(data_dir: &Path) -> Result<Self, CreateDbsError> {
        let db_dir = data_dir.join("./bip300301_enforcer.mdb");
        if let Err(err) = std::fs::create_dir_all(&db_dir) {
            let err = CreateDbsError::CreateDirectory {
                path: db_dir,
                source: err,
            };
            return Err(err);
        }
        let env = {
            let mut env_opts = EnvOpenOptions::new();
            let _: &mut EnvOpenOptions = env_opts.max_dbs(Self::NUM_DBS);
            unsafe { Env::open(&env_opts, db_dir) }?
        };
        let mut rwtxn = env.write_txn()?;
        let block_hash_to_deposits = env.create_db(&mut rwtxn, "block_hash_to_deposits")?;
        let block_height_to_accepted_bmm_block_hashes =
            env.create_db(&mut rwtxn, "block_height_to_accepted_bmm_block_hashes")?;
        let current_block_height = env.create_db(&mut rwtxn, "current_block_height")?;
        let current_chain_tip = env.create_db(&mut rwtxn, "current_chain_tip")?;
        let data_hash_to_sidechain_proposal =
            env.create_db(&mut rwtxn, "data_hash_to_sidechain_proposal")?;
        let leading_by_50 = env.create_db(&mut rwtxn, "leading_by_50")?;
        let previous_votes = env.create_db(&mut rwtxn, "previous_votes")?;
        let sidechain_number_sequence_number_to_treasury_utxo = env.create_db(
            &mut rwtxn,
            "sidechain_number_sequence_number_to_treasury_utxo",
        )?;
        let sidechain_number_to_ctip = env.create_db(&mut rwtxn, "sidechain_number_to_ctip")?;
        let sidechain_number_to_pending_m6ids =
            env.create_db(&mut rwtxn, "sidechain_number_to_pending_m6ids")?;
        let sidechain_number_to_sidechain =
            env.create_db(&mut rwtxn, "sidechain_number_to_sidechain")?;
        let sidechain_number_to_treasury_utxo_count =
            env.create_db(&mut rwtxn, "sidechain_number_to_treasury_utxo_count")?;
        let () = rwtxn.commit()?;
        Ok(Self {
            env,
            block_hash_to_deposits,
            block_height_to_accepted_bmm_block_hashes,
            current_block_height,
            current_chain_tip,
            data_hash_to_sidechain_proposal,
            _leading_by_50: leading_by_50,
            _previous_votes: previous_votes,
            sidechain_number_sequence_number_to_treasury_utxo,
            sidechain_number_to_ctip,
            sidechain_number_to_pending_m6ids,
            sidechain_number_to_sidechain,
            sidechain_number_to_treasury_utxo_count,
        })
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_>, ReadTxnError> {
        self.env.read_txn()
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>, WriteTxnError> {
        self.env.write_txn()
    }
}
