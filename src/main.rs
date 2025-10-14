mod storage;
mod common;

use std::sync::{Arc};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// KV 数据库服务器
pub struct KvServer {
    api: Arc<common::RawKeyValueApi>,
}

impl KvServer {
    pub fn new(storage_path: &str) -> Result<Self, String> {
        let storage = Arc::new(storage::StandaloneStorage::open(storage_path)?);
        let api = Arc::new(common::RawKeyValueApi::new(storage));
        Ok(KvServer { api })
    }

    pub fn start(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr)?;
        println!("KV Server listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let api = Arc::clone(&self.api);
                    thread::spawn(move || {
                        if let Err(e) = Self::handle_client(stream, api) {
                            eprintln!("Error handling client: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                }
            }
        }

        Ok(())
    }

    fn handle_client(
        mut stream: TcpStream,
        api: Arc<common::RawKeyValueApi>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = vec![0; 8192];

        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            let cmd: common::Command = serde_json::from_slice(&buffer[..n])?;
            println!("{}", cmd);
            let response: common::Response = api.handle_command(cmd);
            
            let response_json = serde_json::to_vec(&response)?;
            stream.write_all(&response_json)?;
        }

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = KvServer::new("./kv_data")?;
    server.start("127.0.0.1:8080")?;
    Ok(())
}
