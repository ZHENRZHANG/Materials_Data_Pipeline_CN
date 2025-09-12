# 从POSCAR等文件转化为MongoDB数据库文档
# 插入文档到 MongoDB
from turtledemo.penrose import start
import pandas as pd

from pymongo import MongoClient
import pprint
import os
import shutil
from collections import Counter
import json
from pprint import pprint
import json
import csv
import json
import json

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]

a = "分割线，快速删除值================================================================"
ids_to_delete = ['ID-604', 'ID-606']

# 3. 先查询要删除的文档（确认信息）
query = {"entry_id": {"$in": ids_to_delete}}
docs_to_delete = list(collection.find(query))

if docs_to_delete:
    print(f"即将删除以下 {len(docs_to_delete)} 条记录：")
    for doc in docs_to_delete:
        pprint(doc)
        print("-" * 60)

    delete_result = collection.delete_many(query)
    print(f"\n成功删除 {delete_result.deleted_count} 条记录")
else:
    print(f"未找到ID为 {ids_to_delete} 的记录")
