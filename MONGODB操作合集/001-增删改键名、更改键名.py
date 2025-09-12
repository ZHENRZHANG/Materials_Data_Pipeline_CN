
from pymongo import MongoClient
import pprint
import os
import shutil
from datetime import datetime, timezone
import json

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]

# 获取当前 UTC 时间，并格式化为带时区的 ISO 格式（保留到分钟）
# %z 会自动添加 +00:00 标识（UTC 时区）
current_utc_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M%z")

print(f"国际通用时间: {current_utc_time}")

# ================================强行加入键值对

# collection.update_many(
#     {},  # 空条件表示匹配所有文档
#     {"$set": {"datatime": current_utc_time}}
# )

# collection.update_many(
#             {},  # 匹配所有文档
#             {"$unset": {"datatime": ""}}  # 删除 datatime 字段
#         )

# ================================修改键值名

# collection.update_many(
#     {},  # 匹配所有文档
#     {"$rename": {
#         "structure.Superconductivity_related_properties.Energy_above_hull":
#         "structure.Superconductivity_related_properties.energy_above_hull"
#     }}
# )
