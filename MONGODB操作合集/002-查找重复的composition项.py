# 从POSCAR等文件转化为MongoDB数据库文档
# 插入文档到 MongoDB
from turtledemo.penrose import start

from pymongo import MongoClient
import pprint
import os
import shutil
from collections import Counter
import json


# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]




# 2. 只取需要的两个字段，减少网络传输
# 只取需要的字段
docs = list(collection.find({}, {"_id": 0, "entry_id": 1, "composition": 1}))

# 把 composition 变成可哈希的字符串
def _hashable(comp):
    if isinstance(comp, dict):
        # 字典 → 按字母序排序后转成 json 字符串
        return json.dumps(comp, sort_keys=True)
    else:
        # 字符串/元组直接返回
        return str(comp)

# 统计
comp_counter = Counter(_hashable(d["composition"]) for d in docs)

duplicates = {comp: cnt for comp, cnt in comp_counter.items() if cnt > 1}
if not duplicates:
    print("✅ 无重复 composition。")
else:
    print("🔍 发现重复：")
    for comp_str, cnt in duplicates.items():
        # 反解成原始形式打印
        original_comp = json.loads(comp_str) if comp_str.startswith("{") else comp_str
        print(f"  {original_comp} → 出现 {cnt} 次")
        entry_ids = [d["entry_id"] for d in docs if _hashable(d["composition"]) == comp_str]
        print(f"    entry_id: {entry_ids}")