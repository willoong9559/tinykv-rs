use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;
use std::path::Path;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use serde::{Serialize, Deserialize};
use std::fs;

/// 列族分隔符
const CF_SEPARATOR: &str = "_";

/// 修改操作类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModifyOp {
    Put,
    Delete,
}

/// 单个修改操作
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Modify {
    pub op: ModifyOp,
    pub cf: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Modify {
    pub fn new_put(cf: String, key: Vec<u8>, value: Vec<u8>) -> Self {
        Modify {
            op: ModifyOp::Put,
            cf,
            key,
            value,
        }
    }

    pub fn new_delete(cf: String, key: Vec<u8>) -> Self {
        Modify {
            op: ModifyOp::Delete,
            cf,
            key,
            value: Vec::new(),
        }
    }
}

/// 请求命令
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Get {
        cf: String,
        key: Vec<u8>,
    },
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
    Scan {
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
    },
    Info,
    Flush,
    Compact,
}

/// 响应结果
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Values(Vec<(Vec<u8>, Vec<u8>)>),
    Error(String),
    Info {
        total_keys: usize,
        column_families: Vec<String>,
    },
}

/// 为键添加列族前缀
fn key_with_cf(cf: &str, key: &[u8]) -> Vec<u8> {
    let mut prefixed = cf.as_bytes().to_vec();
    prefixed.extend_from_slice(CF_SEPARATOR.as_bytes());
    prefixed.extend_from_slice(key);
    prefixed
}

/// 从带前缀的键中提取原始键
fn strip_cf_prefix<'a>(cf: &str, key: &'a [u8]) -> Option<&'a [u8]> {
    let prefix = format!("{}{}", cf, CF_SEPARATOR);
    let prefix_bytes = prefix.as_bytes();
    
    if key.starts_with(prefix_bytes) {
        Some(&key[prefix_bytes.len()..])
    } else {
        None
    }
}

/// 存储读取器接口
pub trait StorageReader {
    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn scan_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;
}

/// 独立存储读取器
struct StandaloneStorageReader {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl StorageReader for StandaloneStorageReader {
    fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let prefixed_key = key_with_cf(cf, key);
        let data = self.data.read().map_err(|e| e.to_string())?;
        Ok(data.get(&prefixed_key).cloned())
    }

    fn scan_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let data = self.data.read().map_err(|e| e.to_string())?;
        let prefixed_start = key_with_cf(cf, start_key);
        let prefixed_end = end_key.map(|k| key_with_cf(cf, k));

        let mut results = Vec::new();
        
        for (k, v) in data.iter() {
            if k < &prefixed_start {
                continue;
            }

            if let Some(ref end) = prefixed_end {
                if k >= end {
                    break;
                }
            }

            if let Some(original_key) = strip_cf_prefix(cf, k) {
                results.push((original_key.to_vec(), v.clone()));
                
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }
}

/// 独立存储引擎
pub struct StandaloneStorage {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    path: String,
}

impl StandaloneStorage {
    pub fn new() -> Self {
        StandaloneStorage {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            path: String::new(),
        }
    }

    pub fn open(path: &str) -> Result<Self, String> {
        let storage = StandaloneStorage {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            path: path.to_string(),
        };
        storage.load_from_disk()?;
        Ok(storage)
    }

    pub fn write(&self, batch: Vec<Modify>) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        for modify in batch {
            let prefixed_key = key_with_cf(&modify.cf, &modify.key);

            match modify.op {
                ModifyOp::Put => {
                    data.insert(prefixed_key, modify.value);
                }
                ModifyOp::Delete => {
                    data.remove(&prefixed_key);
                }
            }
        }

        Ok(())
    }

    pub fn reader(&self) -> Result<Box<dyn StorageReader>, String> {
        Ok(Box::new(StandaloneStorageReader {
            data: Arc::clone(&self.data),
        }))
    }

    pub fn flush(&self) -> Result<(), String> {
        self.save_to_disk()
    }

    pub fn save_to_disk(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Ok(());
        }

        let data = self.data.read().map_err(|e| e.to_string())?;
        
        fs::create_dir_all(&self.path)
            .map_err(|e| format!("Failed to create directory: {}", e))?;

        let file_path = format!("{}/data.json", self.path);
        let json = serde_json::to_string_pretty(&*data)
            .map_err(|e| format!("Failed to serialize: {}", e))?;
        
        fs::write(&file_path, json)
            .map_err(|e| format!("Failed to write file: {}", e))?;

        Ok(())
    }

    pub fn load_from_disk(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Ok(());
        }

        let file_path = format!("{}/data.json", self.path);
        if !Path::new(&file_path).exists() {
            return Ok(());
        }

        let json = fs::read_to_string(&file_path)
            .map_err(|e| format!("Failed to read file: {}", e))?;
        
        let data: BTreeMap<Vec<u8>, Vec<u8>> = serde_json::from_str(&json)
            .map_err(|e| format!("Failed to deserialize: {}", e))?;

        let mut storage_data = self.data.write().map_err(|e| e.to_string())?;
        *storage_data = data;

        Ok(())
    }

    pub fn get_stats(&self) -> Result<(usize, Vec<String>), String> {
        let data = self.data.read().map_err(|e| e.to_string())?;
        
        let mut cfs = std::collections::HashSet::new();
        for key in data.keys() {
            if let Some(sep_pos) = key.iter().position(|&b| b == b'_') {
                if let Ok(cf) = std::str::from_utf8(&key[..sep_pos]) {
                    cfs.insert(cf.to_string());
                }
            }
        }

        let mut cf_list: Vec<String> = cfs.into_iter().collect();
        cf_list.sort();

        Ok((data.len(), cf_list))
    }
}

impl Default for StandaloneStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// 原始键值API
pub struct RawKeyValueApi {
    storage: Arc<StandaloneStorage>,
}

impl RawKeyValueApi {
    pub fn new(storage: Arc<StandaloneStorage>) -> Self {
        RawKeyValueApi { storage }
    }

    pub fn raw_get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let reader = self.storage.reader()?;
        reader.get_cf(cf, key)
    }

    pub fn raw_put(&self, cf: String, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let modify = Modify::new_put(cf, key, value);
        self.storage.write(vec![modify])
    }

    pub fn raw_delete(&self, cf: String, key: Vec<u8>) -> Result<(), String> {
        let modify = Modify::new_delete(cf, key);
        self.storage.write(vec![modify])
    }

    pub fn raw_scan(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let reader = self.storage.reader()?;
        reader.scan_cf(cf, start_key, end_key, limit)
    }

    pub fn handle_command(&self, cmd: Command) -> Response {
        match cmd {
            Command::Get { cf, key } => {
                match self.raw_get(&cf, &key) {
                    Ok(value) => Response::Value(value),
                    Err(e) => Response::Error(e),
                }
            }
            Command::Put { cf, key, value } => {
                match self.raw_put(cf, key, value) {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            Command::Delete { cf, key } => {
                match self.raw_delete(cf, key) {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            Command::Scan { cf, start_key, end_key, limit } => {
                match self.raw_scan(&cf, &start_key, end_key.as_deref(), limit) {
                    Ok(values) => Response::Values(values),
                    Err(e) => Response::Error(e),
                }
            }
            Command::Info => {
                match self.storage.get_stats() {
                    Ok((total_keys, cfs)) => Response::Info {
                        total_keys,
                        column_families: cfs,
                    },
                    Err(e) => Response::Error(e),
                }
            }
            Command::Flush => {
                match self.storage.flush() {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Error(e),
                }
            }
            Command::Compact => {
                Response::Ok
            }
        }
    }
}

/// KV 数据库服务器
pub struct KvServer {
    api: Arc<RawKeyValueApi>,
}

impl KvServer {
    pub fn new(storage_path: &str) -> Result<Self, String> {
        let storage = Arc::new(StandaloneStorage::open(storage_path)?);
        let api = Arc::new(RawKeyValueApi::new(storage));
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
        api: Arc<RawKeyValueApi>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = vec![0; 8192];

        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            let cmd: Command = serde_json::from_slice(&buffer[..n])?;
            let response = api.handle_command(cmd);
            
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        api.raw_put("default".to_string(), b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let value = api.raw_get("default", b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_delete() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        api.raw_put("default".to_string(), b"key1".to_vec(), b"value1".to_vec()).unwrap();
        api.raw_delete("default".to_string(), b"key1".to_vec()).unwrap();
        let value = api.raw_get("default", b"key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_column_families() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        api.raw_put("cf1".to_string(), b"key".to_vec(), b"value1".to_vec()).unwrap();
        api.raw_put("cf2".to_string(), b"key".to_vec(), b"value2".to_vec()).unwrap();

        let v1 = api.raw_get("cf1", b"key").unwrap();
        let v2 = api.raw_get("cf2", b"key").unwrap();

        assert_eq!(v1, Some(b"value1".to_vec()));
        assert_eq!(v2, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_scan() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            api.raw_put("default".to_string(), key.into_bytes(), value.into_bytes()).unwrap();
        }

        let results = api.raw_scan("default", b"key1", Some(b"key4"), 10).unwrap();
        assert_eq!(results.len(), 3);
    }
}