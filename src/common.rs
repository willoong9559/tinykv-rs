use crate::storage;

use std::sync::{Arc};
use std::fmt;
use serde::{Serialize, Deserialize};

/// 列族分隔符
pub const CF_SEPARATOR: &str = "_";

// 修改操作类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModifyOp {
    Put,
    Delete,
}

// 单个修改操作
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

// 请求命令
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] 
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

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Get { cf, key } => {
                write!(f, "Get(cf: {}, key: {})", cf, String::from_utf8_lossy(key))
            }
            Command::Put { cf, key, value } => {
                write!(
                    f,
                    "Put(cf: {}, key: {}, value: {})",
                    cf,
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                )
            }
            Command::Delete { cf, key } => {
                write!(f, "Delete(cf: {}, key: {})", cf, String::from_utf8_lossy(key))
            }
            Command::Scan { cf, start_key, end_key, limit } => {
                let end_key_str = match end_key {
                    Some(k) => String::from_utf8_lossy(k).into_owned(),
                    None => "None".to_string(),
                };
                write!(
                    f,
                    "Scan(cf: {}, start_key: {}, end_key: {}, limit: {})",
                    cf,
                    String::from_utf8_lossy(start_key),
                    end_key_str,
                    limit
                )
            }
            Command::Info => write!(f, "Info"),
            Command::Flush => write!(f, "Flush"),
            Command::Compact => write!(f, "Compact"),
        }
    }
}

// 响应结果
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

// 为键添加列族前缀
pub fn key_with_cf(cf: &str, key: &[u8]) -> Vec<u8> {
    let mut prefixed = cf.as_bytes().to_vec();
    prefixed.extend_from_slice(CF_SEPARATOR.as_bytes());
    prefixed.extend_from_slice(key);
    prefixed
}

// 从带前缀的键中提取原始键
pub fn strip_cf_prefix<'a>(cf: &str, key: &'a [u8]) -> Option<&'a [u8]> {
    let prefix = format!("{}{}", cf, CF_SEPARATOR);
    let prefix_bytes = prefix.as_bytes();
    
    if key.starts_with(prefix_bytes) {
        Some(&key[prefix_bytes.len()..])
    } else {
        None
    }
}

impl Default for storage::StandaloneStorage {
    fn default() -> Self {
        Self::new()
    }
}

// 原始键值API
pub struct RawKeyValueApi {
    storage: Arc<storage::StandaloneStorage>,
}

impl RawKeyValueApi {
    pub fn new(storage: Arc<storage::StandaloneStorage>) -> Self {
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