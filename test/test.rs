#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        // 测试 Put
        api.raw_put("default".to_string(), b"key1".to_vec(), b"value1".to_vec()).unwrap();

        // 测试 Get
        let value = api.raw_get("default", b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // 测试 Delete
        api.raw_delete("default".to_string(), b"key1".to_vec()).unwrap();
        let value = api.raw_get("default", b"key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_column_families() {
        let storage = Arc::new(StandaloneStorage::new());
        let api = RawKeyValueApi::new(storage);

        // 同一键在不同列族中存储不同值
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
        assert_eq!(results.len(), 3); // key1, key2, key3
    }
}