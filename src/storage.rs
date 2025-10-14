use crate::common;

use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

// 独立存储引擎
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

    pub fn write(&self, batch: Vec<common::Modify>) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        for modify in batch {
            let prefixed_key = common::key_with_cf(&modify.cf, &modify.key);

            match modify.op {
                common::ModifyOp::Put => {
                    data.insert(prefixed_key, modify.value);
                }
                common::ModifyOp::Delete => {
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
        let prefixed_key = common::key_with_cf(cf, key);
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
        let prefixed_start = common::key_with_cf(cf, start_key);
        let prefixed_end = end_key.map(|k| common::key_with_cf(cf, k));

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

            if let Some(original_key) = common::strip_cf_prefix(cf, k) {
                results.push((original_key.to_vec(), v.clone()));
                
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }
}