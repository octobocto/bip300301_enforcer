use bip300301_messages::bitcoin;
use miette::miette;
use serde::Deserialize;
use thiserror::Error;

use crate::cli::Config;

pub use ureq_jsonrpc::Client as RpcClient;

pub fn create_client(conf: &Config) -> Result<RpcClient, miette::Report> {
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

    Ok(RpcClient {
        host: conf.node_rpc_host.to_string(),
        port: conf.node_rpc_port,
        user: conf_user.to_string(),
        password: conf_password.to_string(),
        id: "mainchain".into(),
    })
}

#[derive(Debug, Deserialize)]
pub struct BlockchainInfo {
    #[serde(with = "bitcoin::network::as_core_arg")]
    pub chain: bitcoin::Network,
    pub blocks: u32,
    #[serde(rename = "bestblockhash")]
    pub best_blockhash: bitcoin::BlockHash,
    pub difficulty: f64,
}

#[derive(Debug, Error)]
pub enum GetBlockchainInfoError {
    #[error("No response data for rpc method `getblockchaininfo`")]
    NoResponseData,
    #[error("RPC error: `getblockchaininfo`")]
    Rpc(#[from] ureq_jsonrpc::Error),
}

pub fn get_blockchain_info(client: &RpcClient) -> Result<BlockchainInfo, GetBlockchainInfoError> {
    client
        .send_request("getblockchain", &[])
        .map_err(GetBlockchainInfoError::Rpc)?
        .ok_or(GetBlockchainInfoError::NoResponseData)
}
