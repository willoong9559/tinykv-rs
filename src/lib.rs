pub mod storage;
pub mod common;
pub mod server;
pub mod client;

use std::{error::Error, result};

/// 启动 server
pub fn run_server(data_path: &str, addr: &str) -> Result<(), Box<dyn Error>> {
    let server = server::KvServer::new(data_path)?;
    server.start(addr)?;
    Ok(())
}
