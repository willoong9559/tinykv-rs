use tinykv_rs::client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TinyKV 客户端示例 ===\n");

    // 连接到服务器
    let mut client = client::KvClient::connect("127.0.0.1:8080")?;
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
        let mut client = client::KvClient::connect("127.0.0.1:8080")?;

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
        let mut client = client::KvClient::connect("127.0.0.1:8080")?;

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
        let mut client = client::KvClient::connect("127.0.0.1:8080")?;

        for i in 0..5 {
            client.put("default", &format!("key{}", i), &format!("value{}", i))?;
        }

        let results = client.scan("default", "key0", Some("key3"), 10)?;
        assert!(results.len() > 0);

        Ok(())
    }
}