package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Client TinyKV 客户端
type Client struct {
	conn net.Conn
}

// Command 命令结构
type Command struct {
	Type     string `json:"type"`
	CF       string `json:"cf,omitempty"`
	Key      []byte `json:"key,omitempty"`
	Value    []byte `json:"value,omitempty"`
	StartKey []byte `json:"start_key,omitempty"`
	EndKey   []byte `json:"end_key,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// Response 响应结构
type Response struct {
	Value  interface{}            `json:"Value,omitempty"`
	Values interface{}            `json:"Values,omitempty"`
	Info   map[string]interface{} `json:"Info,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

// NewClient 创建新客户端
func NewClient(address string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}

	return &Client{
		conn: conn,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.conn.Close()
}

// encodeBase64 将字符串编码为 Base64
func encodeBase64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// decodeBase64 将 Base64 解码为字符串
func decodeBase64(s string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func stringToBytes(s string) []byte {
	return []byte(s)
}

// decodeBytes 解码字节数组 (JSON 中的 [98, 111, 98] 格式)
func decodeBytes(data interface{}) (string, error) {
	switch v := data.(type) {
	case []interface{}:
		bytes := make([]byte, len(v))
		for i, b := range v {
			if num, ok := b.(float64); ok {
				bytes[i] = byte(num)
			} else {
				return "", fmt.Errorf("invalid byte value")
			}
		}
		return string(bytes), nil
	case string:
		// 如果是 Base64 字符串
		return decodeBase64(v)
	default:
		return "", fmt.Errorf("unsupported data type: %T", data)
	}
}

// sendCommand 发送命令
func (c *Client) sendCommand(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("序列化命令失败: %w", err)
	}

	// 不添加换行符,直接发送 JSON
	_, err = c.conn.Write(data)
	if err != nil {
		return fmt.Errorf("发送命令失败: %w", err)
	}

	return nil
}

// readResponse 读取响应
func (c *Client) readResponse() (*Response, error) {
	buffer := make([]byte, 8192)
	n, err := c.conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("服务器关闭连接")
		}
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	var resp Response
	if err := json.Unmarshal(buffer[:n], &resp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return &resp, nil
}

// Put 存储键值对 (Base64 编码)
func (c *Client) Put(cf, key, value string) error {
	cmd := Command{
		Type:  "Put",
		CF:    cf,
		Key:   stringToBytes(key),
		Value: stringToBytes(value),
	}

	if err := c.sendCommand(cmd); err != nil {
		return err
	}

	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("Put 失败: %s", resp.Error)
	}

	return nil
}

// Get 获取值 (Base64 解码)
func (c *Client) Get(cf, key string) (string, bool, error) {
	cmd := Command{
		Type: "Get",
		CF:   cf,
		Key:  []byte(key),
	}

	if err := c.sendCommand(cmd); err != nil {
		return "", false, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", false, err
	}

	if resp.Error != "" {
		return "", false, fmt.Errorf("Get 失败: %s", resp.Error)
	}

	// 检查是否找到值
	if resp.Value == nil {
		return "", false, nil
	}

	// 检查是否是空数组
	if arr, ok := resp.Value.([]interface{}); ok && len(arr) == 0 {
		return "", false, nil
	}

	// 解码值
	value, err := decodeBytes(resp.Value)
	if err != nil {
		return "", false, fmt.Errorf("解码值失败: %w", err)
	}

	return value, true, nil
}

// Delete 删除键 (Base64 编码)
func (c *Client) Delete(cf, key string) error {
	cmd := Command{
		Type: "Delete",
		CF:   cf,
		Key:  stringToBytes(key),
	}

	if err := c.sendCommand(cmd); err != nil {
		return err
	}

	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("Delete 失败: %s", resp.Error)
	}

	return nil
}

// Scan 扫描范围 (Base64 编码)
func (c *Client) Scan(cf, startKey string, endKey *string, limit int) ([]map[string]string, error) {
	var encodedEndKey string
	if endKey != nil {
		encoded := encodeBase64(*endKey)
		encodedEndKey = encoded
	}

	cmd := Command{
		Type:     "Scan",
		CF:       cf,
		StartKey: stringToBytes(startKey),
		EndKey:   stringToBytes(encodedEndKey),
		Limit:    limit,
	}

	if err := c.sendCommand(cmd); err != nil {
		return nil, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("Scan 失败: %s", resp.Error)
	}

	// 解析结果 [[key_bytes, value_bytes], ...]
	var result []map[string]string
	if resp.Values != nil {
		valuesArr, ok := resp.Values.([]interface{})
		if !ok {
			return nil, fmt.Errorf("Scan 响应格式错误")
		}

		for _, item := range valuesArr {
			itemArr, ok := item.([]interface{})
			if !ok || len(itemArr) != 2 {
				continue
			}

			key, err := decodeBytes(itemArr[0])
			if err != nil {
				continue
			}

			value, err := decodeBytes(itemArr[1])
			if err != nil {
				continue
			}

			result = append(result, map[string]string{
				"key":   key,
				"value": value,
			})
		}
	}

	return result, nil
}

// Info 获取服务器信息
func (c *Client) Info() (int, []string, error) {
	cmd := Command{
		Type: "Info",
	}

	if err := c.sendCommand(cmd); err != nil {
		return 0, nil, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, nil, err
	}

	if resp.Error != "" {
		return 0, nil, fmt.Errorf("Info 失败: %s", resp.Error)
	}

	if resp.Info == nil {
		return 0, nil, fmt.Errorf("Info 响应为空")
	}

	totalKeys := 0
	if tk, ok := resp.Info["total_keys"].(float64); ok {
		totalKeys = int(tk)
	}

	var cfs []string
	if cfList, ok := resp.Info["column_families"].([]interface{}); ok {
		for _, cf := range cfList {
			if cfStr, ok := cf.(string); ok {
				cfs = append(cfs, cfStr)
			}
		}
	}

	return totalKeys, cfs, nil
}

// Flush 刷盘
func (c *Client) Flush() error {
	cmd := Command{
		Type: "Flush",
	}

	if err := c.sendCommand(cmd); err != nil {
		return err
	}

	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("Flush 失败: %s", resp.Error)
	}

	return nil
}

// ===================== 示例代码 =====================

func main() {
	fmt.Println("=== TinyKV Go 客户端示例 (匹配 Rust 客户端) ===")

	// 连接服务器
	client, err := NewClient("127.0.0.1:8080")
	if err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		return
	}
	defer client.Close()
	fmt.Println("✓ 已连接到服务器: 127.0.0.1:8080")

	// 示例 1: 基本 Put/Get 操作
	fmt.Println("\n【示例 1】基本 Put/Get 操作")
	fmt.Println("--------------------------------------------------")

	if err := client.Put("default", "name", "Alice"); err != nil {
		fmt.Printf("Put 错误: %v\n", err)
	} else {
		fmt.Println("✓ Put: cf=default, key=name, value=Alice")
	}

	if value, found, err := client.Get("default", "name"); err != nil {
		fmt.Printf("Get 错误: %v\n", err)
	} else if found {
		fmt.Printf("✓ Get: cf=default, key=name -> %s\n", value)
	} else {
		fmt.Println("✓ Get: cf=default, key=name -> 键不存在")
	}

	// 示例 2: 中文支持
	fmt.Println("\n【示例 2】中文支持")
	fmt.Println("--------------------------------------------------")

	if err := client.Put("default", "城市", "北京"); err != nil {
		fmt.Printf("Put 错误: %v\n", err)
	} else {
		fmt.Println("✓ Put: 城市=北京")
	}

	if value, found, err := client.Get("default", "城市"); err != nil {
		fmt.Printf("Get 错误: %v\n", err)
	} else if found {
		fmt.Printf("✓ Get: 城市 -> %s\n", value)
	}

	// 示例 3: Delete 操作
	fmt.Println("\n【示例 3】Delete 操作")
	fmt.Println("--------------------------------------------------")

	if err := client.Put("default", "temp", "temporary value"); err != nil {
		fmt.Printf("Put 错误: %v\n", err)
	} else {
		fmt.Println("✓ Put: temp=temporary value")
	}

	if err := client.Delete("default", "temp"); err != nil {
		fmt.Printf("Delete 错误: %v\n", err)
	} else {
		fmt.Println("✓ Delete: 已删除 temp")
	}

	if _, found, err := client.Get("default", "temp"); err != nil {
		fmt.Printf("Get 错误: %v\n", err)
	} else if !found {
		fmt.Println("✓ Get: temp -> 键不存在 (删除成功)")
	}

	// 示例 4: Scan 范围扫描
	fmt.Println("\n【示例 4】Scan 范围扫描")
	fmt.Println("--------------------------------------------------")

	// 插入测试数据
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := client.Put("default", key, value); err != nil {
			fmt.Printf("Put 错误: %v\n", err)
		}
	}
	fmt.Println("✓ 已插入 key1-key5")

	endKey := "key9"
	results, err := client.Scan("default", "key1", &endKey, 10)
	if err != nil {
		fmt.Printf("Scan 错误: %v\n", err)
	} else {
		fmt.Printf("✓ Scan 结果 (找到 %d 个键):\n", len(results))
		for _, item := range results {
			fmt.Printf("  %s = %s\n", item["key"], item["value"])
		}
	}

	// 示例 5: 服务器信息
	fmt.Println("\n【示例 5】获取服务器信息")
	fmt.Println("--------------------------------------------------")

	totalKeys, cfs, err := client.Info()
	if err != nil {
		fmt.Printf("Info 错误: %v\n", err)
	} else {
		fmt.Printf("✓ 服务器信息:\n")
		fmt.Printf("  总键数: %d\n", totalKeys)
		fmt.Printf("  列族: %v\n", cfs)
	}

	// 示例 6: 刷盘
	fmt.Println("\n【示例 6】刷盘持久化")
	fmt.Println("--------------------------------------------------")

	if err := client.Flush(); err != nil {
		fmt.Printf("Flush 错误: %v\n", err)
	} else {
		fmt.Println("✓ Flush: 数据已刷盘")
	}

	// 示例 7: 批量操作
	fmt.Println("\n【示例 7】批量操作")
	fmt.Println("--------------------------------------------------")

	users := map[string]string{
		"user:1": "Alice",
		"user:2": "Bob",
		"user:3": "Charlie",
	}

	for key, value := range users {
		if err := client.Put("default", key, value); err != nil {
			fmt.Printf("批量 Put 错误: %v\n", err)
		}
	}
	fmt.Printf("✓ 批量写入 %d 条记录\n", len(users))

	// 验证批量写入
	for key := range users {
		if value, found, err := client.Get("default", key); err != nil {
			fmt.Printf("验证错误: %v\n", err)
		} else if found {
			fmt.Printf("  %s = %s\n", key, value)
		}
	}
}
