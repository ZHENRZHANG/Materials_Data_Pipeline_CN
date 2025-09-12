# export_mongo.py
from pymongo import MongoClient
import json
import os
from datetime import datetime
import json
import pandas as pd

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]


OUTPUT_DIR = ''
OUTPUT_JSON = './export/a.json'
OUTPUT_JSONL = './export/a.jsonl'  # 可选：逐行 JSON
OUTPUT_CSV = './export/a.csv'     # 可选：CSV（需结构规整）

# 创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)



# ========== 读取所有文档 ==========
print("正在从 MongoDB 读取数据...")
documents = list(collection.find({}))  # 获取所有文档
print(f"共读取到 {len(documents)} 条文档。")
for doc in documents:
    doc['_id'] = str(doc['_id'])

# ========== 转换 ObjectId 和 datetime 为 JSON 可序列化格式 ==========
def serialize_doc(doc):
    """递归处理 ObjectId 和 datetime"""
    if isinstance(doc, dict):
        return {k: serialize_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [serialize_doc(i) for i in doc]
    elif isinstance(doc, datetime):
        return doc.isoformat() + 'Z'  # 标准 ISO 格式
    elif hasattr(doc, '_id'):
        doc['_id'] = str(doc['_id'])  # ObjectId 转字符串
        return doc
    else:
        return doc

# 序列化所有文档
serialized_docs = serialize_doc(documents)

# ========== 写入 JSON 文件 ==========
print(f"正在写入 JSON 文件: {OUTPUT_JSON}")
with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(serialized_docs, f, ensure_ascii=False, indent=2)

print(f"✅ 成功导出到 {OUTPUT_JSON}")

# ========== 可选：写入 JSONL（每行一个 JSON 对象）==========
print(f"正在写入 JSONL 文件: {OUTPUT_JSONL}")
with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
    for doc in serialized_docs:
        f.write(json.dumps(doc, ensure_ascii=False) + '\n')
print(f"✅ 成功导出到 {OUTPUT_JSONL}")

# ========== 可选：写入 CSV（仅当数据结构规整时）==========
try:
    df = pd.json_normalize(serialized_docs)  # 展平嵌套结构
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"✅ 成功导出到 {OUTPUT_CSV}")
except Exception as e:
    print(f"❌ 导出 CSV 失败（可能结构不规整）: {e}")

# ========== 关闭连接 ==========
client.close()
print("导出完成。")