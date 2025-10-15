import json
import socket

class KvClient:
    def __init__(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
    
    def put(self, cf, key, value):
        cmd = {
            "Put": {
                "cf": cf,
                "key": list(key.encode()),
                "value": list(value.encode())
            }
        }
        self.socket.send(json.dumps(cmd).encode())
        response = self.socket.recv(1024)
        return json.loads(response)
    
    def get(self, cf, key):
        cmd = {
            "Get": {
                "cf": cf,
                "key": list(key.encode())
            }
        }
        self.socket.send(json.dumps(cmd).encode())
        response = self.socket.recv(1024)
        return json.loads(response)
    
    def close(self):
        self.socket.close()

# 使用示例
client = KvClient('127.0.0.1', 8080)
client.put('default', 'name', 'Alice')
result = client.get('default', 'name')
print(result)  # {'Value': [65, 108, 105, 99, 101]}
client.close()