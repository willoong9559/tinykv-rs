mod server;
mod common;
mod storage;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::run_server("./kv_data", "127.0.0.1:8080")
}
