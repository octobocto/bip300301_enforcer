use std::{net::SocketAddr, path::Path};

mod bip300;
mod cli;
mod proto;
mod server;
mod types;

use clap::Parser;
use futures::{
    future::{self, Either},
    FutureExt, TryFutureExt,
};
use miette::{miette, IntoDiagnostic, Result};
use proto::mainchain;
use server::Bip300;
use tonic::transport::Server;
use ureq_jsonrpc::Client;

fn _create_client(main_datadir: &Path) -> Result<Client> {
    let auth = std::fs::read_to_string(main_datadir.join("regtest/.cookie")).into_diagnostic()?;
    let mut auth = auth.split(':');
    let user = auth
        .next()
        .ok_or(miette!("failed to get rpcuser"))?
        .to_string();
    let password = auth
        .next()
        .ok_or(miette!("failed to get rpcpassword"))?
        .to_string();
    Ok(Client {
        host: "localhost".into(),
        port: 18443,
        user,
        password,
        id: "mainchain".into(),
    })
}

async fn run_server(bip300: Bip300, addr: SocketAddr) -> Result<()> {
    println!("Listening for gRPC on {addr}");
    Server::builder()
        .add_service(mainchain::Server::new(bip300))
        .serve(addr)
        .map(|res| res.into_diagnostic())
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Config::parse();
    let serve_rpc_addr = cli.serve_rpc_addr;

    let bip300 = Bip300::new(Path::new("./")).into_diagnostic()?;

    let task = bip300
        .run(cli)
        .map(|res| res.into_diagnostic())
        .map_err(|err| miette!("unable to initialize bip300 handler: {}", err))
        .unwrap_or_else(|err| eprintln!("{err:#}"));

    //let ((), ()) = future::try_join(task.map(Ok), run_server(bip300, addr)).await?;
    match future::select(task, run_server(bip300, serve_rpc_addr).boxed()).await {
        Either::Left(((), server_task)) => {
            // continue to run server task
            server_task.await
        }
        Either::Right((res, _task)) => res,
    }
}
