import oss2
import json
import os

# 读取配置文件（不提交到GitHub的敏感配置）
with open("oss_config.json", "r") as f:
    config = json.load(f)["oss"]

# 从配置中获取信息（不再硬编码）
auth = oss2.Auth(config["access_key_id"], config["access_key_secret"])
bucket = oss2.Bucket(auth, config["endpoint"], config["bucket_name"])

# 生成签名URL（有效期设为合理值，如600秒）
url = bucket.sign_url('GET', 'example.jpg', 600)
print(url)