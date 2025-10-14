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
            "Get": {
                "cf": cf,
                "key": key.as_bytes()
            }
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
            "Put": {
                "cf": cf,
                "key": key.as_bytes(),
                "value": value.as_bytes()
            }
        });

        self.send_command(&cmd)?;
        self.read_response()?;
        Ok(())
    }

    /// Delete 操作：删除键
    pub fn delete(&mut self, cf: &str, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = json!({
            "Delete": {
                "cf": cf,
                "key": key.as_bytes()
            }
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
            "Scan": {
                "cf": cf,
                "start_key": start_key.as_bytes(),
                "end_key": end_key.map(|k| k.as_bytes()),
                "limit": limit
            }
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
        let cmd = json!({ "Info": {} });
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
        let cmd = json!({ "Flush": {} });
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TinyKV 客户端示例 ===\n");

    // 连接到服务器
    let mut client = KvClient::connect("127.0.0.1:8080")?;
    println!("✓ 已连接到服务器: 127.0.0.1:8080\n");

    // ===== 示例 1: 基本的 Put/Get 操作 =====
    println!("【示例 1】基本 Put/Get 操作");
    println!("{}", "-".repeat(50));
    
    client.put("default", "name", "Alice")?;
    println!("✓ Put: cf=default, key=name, value=Alice");
    
    client.put("default", "age", "30")?;
    println!("✓ Put: cf=default, key=age, value=30");
    
    let name = client.get("default", "name")?;
    println!("✓ Get: cf=default, key=name -> {:?}", name);
    
    let age = client.get("default", "age")?;
    println!("✓ Get: cf=default, key=age -> {:?}\n", age);

    // ===== 示例 2: Delete 操作 =====
    println!("【示例 2】Delete 操作");
    println!("{}", "-".repeat(50));    
    client.delete("default", "age")?;
    println!("✓ Delete: cf=default, key=age");
    
    let age = client.get("default", "age")?;
    println!("✓ Get: cf=default, key=age -> {:?}\n", age);

    // ===== 示例 3: 列族隔离 =====
    println!("【示例 3】列族隔离");
    println!("{}", "-".repeat(50));    
    client.put("users", "user1", "Bob")?;
    client.put("users", "user2", "Charlie")?;
    println!("✓ Put 到 users 列族: user1=Bob, user2=Charlie");
    
    client.put("products", "product1", "Laptop")?;
    client.put("products", "product2", "Mouse")?;
    println!("✓ Put 到 products 列族: product1=Laptop, product2=Mouse");
    
    let user = client.get("users", "user1")?;
    let product = client.get("products", "product1")?;
    println!("✓ Get users.user1 -> {:?}", user);
    println!("✓ Get products.product1 -> {:?}\n", product);

    // ===== 示例 4: Scan 操作 =====
    println!("【示例 4】Scan 范围扫描");
    println!("{}", "-".repeat(50));    
    // 添加多个键值对用于扫描
    for i in 1..=5 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        client.put("default", &key, &value)?;
    }
    println!("✓ 添加了 5 个键值对: key1-key5");
    
    let results = client.scan("default", "key1", Some("key4"), 10)?;
    println!("✓ Scan from key1 to key4 (limit=10):");
    for (k, v) in &results {
        println!("  - {}: {}", k, v);
    }
    println!();

    // ===== 示例 5: Info 操作 =====
    println!("【示例 5】获取服务器统计信息");
    println!("{}", "-".repeat(50));

    let (total_keys, cfs) = client.info()?;
    println!("✓ 总键数: {}", total_keys);
    println!("✓ 列族列表: {:?}\n", cfs);

    // ===== 示例 6: Flush 持久化 =====
    println!("【示例 6】刷盘持久化");
    println!("{}", "-".repeat(50));
    
    client.flush()?;
    println!("✓ 数据已持久化到磁盘\n");

    // ===== 示例 7: 批量操作模式 =====
    println!("【示例 7】批量操作");
    println!("{}", "-".repeat(50));
    
    let data = vec![
        ("email", "alice@example.com"),
        ("phone", "123-456-7890"),
        ("address", "123 Main St"),
    ];
    
    for (key, value) in &data {
        client.put("user_info", key, value)?;
    }
    println!("✓ 批量写入 {} 条记录", data.len());
    
    for (key, _) in &data {
        if let Some(value) = client.get("user_info", key)? {
            println!("  - {}: {}", key, value);
        }
    }
    println!();

    // ===== 示例 8: 错误处理 =====
    println!("【示例 8】空值处理");
    println!("{}", "-".repeat(50));
    
    let non_existent = client.get("default", "non_existent_key")?;
    println!("✓ 获取不存在的键: {:?}", non_existent);
    println!("✓ 返回 None 表示键不存在\n");

    println!("=== 所有示例执行完成 ===");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // 运行此测试需要启动服务器
    fn test_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = KvClient::connect("127.0.0.1:8080")?;

        // Put and Get
        client.put("test", "key1", "value1")?;
        let value = client.get("test", "key1")?;
        assert_eq!(value, Some("value1".to_string()));

        // Delete
        client.delete("test", "key1")?;
        let value = client.get("test", "key1")?;
        assert_eq!(value, None);

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_multiple_column_families() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = KvClient::connect("127.0.0.1:8080")?;

        client.put("cf1", "k1", "v1")?;
        client.put("cf2", "k1", "v2")?;

        let v1 = client.get("cf1", "k1")?;
        let v2 = client.get("cf2", "k1")?;

        assert_eq!(v1, Some("v1".to_string()));
        assert_eq!(v2, Some("v2".to_string()));

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_scan_operation() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = KvClient::connect("127.0.0.1:8080")?;

        for i in 0..5 {
            client.put("default", &format!("key{}", i), &format!("value{}", i))?;
        }

        let results = client.scan("default", "key0", Some("key3"), 10)?;
        assert!(results.len() > 0);

        Ok(())
    }
}