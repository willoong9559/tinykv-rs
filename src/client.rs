use std::io::{Read, Write};
use std::net::TcpStream;
use serde_json::json;

/// KV 数据库客户端
pub struct KvClient {
    stream: TcpStream,
}

impl KvClient {
    /// 连接到 KV 服务器
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvClient { stream })
    }

    /// Get 操作：获取单个键值
    pub fn get(&mut self, cf: &str, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let cmd = json!({
                "type": "Get",
                "cf": cf,
                "key": key.as_bytes()
        });

        self.send_command(&cmd)?;
        let response = self.read_response()?;

        match response.get("Value") {
            Some(serde_json::Value::Array(arr)) if arr.is_empty() => Ok(None),
            Some(serde_json::Value::Null) => Ok(None),
            Some(value) => {
                let bytes: Vec<u8> = serde_json::from_value(value.clone())?;
                Ok(Some(String::from_utf8(bytes)?))
            }
            None => Ok(None),
        }
    }

    /// Put 操作：写入键值对
    pub fn put(
        &mut self,
        cf: &str,
        key: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = json!({
                "type":"Put",
                "cf": cf,
                "key": key.as_bytes(),
                "value": value.as_bytes()
        });

        self.send_command(&cmd)?;
        self.read_response()?;
        Ok(())
    }

    /// Delete 操作：删除键
    pub fn delete(&mut self, cf: &str, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = json!({
                "type": "Delete",
                "cf": cf,
                "key": key.as_bytes()
        });

        self.send_command(&cmd)?;
        self.read_response()?;
        Ok(())
    }

    /// Scan 操作：范围扫描
    pub fn scan(
        &mut self,
        cf: &str,
        start_key: &str,
        end_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        let cmd = json!({
                "type": "Scan",
                "cf": cf,
                "start_key": start_key.as_bytes(),
                "end_key": end_key.map(|k| k.as_bytes()),
                "limit": limit
        });

        self.send_command(&cmd)?;
        let response = self.read_response()?;

        match response.get("Values") {
            Some(values) => {
                let items: Vec<Vec<Vec<u8>>> = serde_json::from_value(values.clone())?;
                let result = items
                    .into_iter()
                    .map(|item| {
                        (
                            String::from_utf8_lossy(&item[0]).to_string(),
                            String::from_utf8_lossy(&item[1]).to_string(),
                        )
                    })
                    .collect();
                Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    /// 获取服务器信息
    pub fn info(&mut self) -> Result<(usize, Vec<String>), Box<dyn std::error::Error>> {
        let cmd = json!({
            "type": "Info" 
        });
        self.send_command(&cmd)?;
        
        let response = self.read_response()?;
        if let Some(info) = response.get("Info") {
            let total_keys: usize = serde_json::from_value(info["total_keys"].clone())?;
            let cfs: Vec<String> = serde_json::from_value(info["column_families"].clone())?;
            Ok((total_keys, cfs))
        } else {
            Err("Invalid response".into())
        }
    }

    /// 刷盘持久化
    pub fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = json!({
            "type": "Flush"
        });
        self.send_command(&cmd)?;
        self.read_response()?;
        Ok(())
    }

    fn send_command(&mut self, cmd: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_vec(cmd)?;
        self.stream.write_all(&json)?;
        Ok(())
    }

    fn read_response(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut buffer = vec![0; 8192];
        let n = self.stream.read(&mut buffer)?;
        let response = serde_json::from_slice(&buffer[..n])?;
        Ok(response)
    }
}