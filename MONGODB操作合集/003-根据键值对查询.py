
from pymongo import MongoClient
from pprint import pprint
import json

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]


b = "分割线，根据键值对================================================================"
# result = collection.find({'composition.B': 2, 'composition.Sr': 1})
#
result = collection.find({
    'composition.Sc': {'$exists': True},
    'composition.Ti': {'$exists': True},
    'composition.B': {'$exists': True}
})

# 打印查询结果
for doc in result:
    pprint(doc)

