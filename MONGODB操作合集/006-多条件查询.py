"""
🔍 脚本名称：MongoDB 材料数据查询与导出工具

📌 功能概述：
本脚本从 MongoDB 数据库中查询符合特定结构和性质条件的材料数据，筛选出具有 `ThB5(P4/mmm)` 晶体结构、
超导图像编号（img_number）为 0 的材料，并将结果按 entry_id 数值升序排列，最终导出为 CSV 文件。

🎯 查询条件：
- original-structure 必须为 'ThB5(P4/mmm)'
- structure.Superconductivity_related_properties.img_number = 0
- （可选）formation_energy < 0（当前注释中已注释，可按需启用）

📊 输出内容：
- 控制台打印每条匹配记录的：
  - Entry ID
  - 化学组成（Composition）
  - 超导相关性质（形成能、能量凸包、λ值、图像编号、low_three 等）

💾 文件输出：
- 所有查询结果自动整理为结构化表格（DataFrame）
- 保存为：`03-1-superconductivity_data.csv`
- 包含字段：Entry ID, Composition, Formation Energy, Energy Above Hull, Image Number, Lambda Gamma, Low Three


📅 作者：张圳锐
"""
import numpy as np
import pandas as pd
from pymongo import MongoClient
import json

with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]






d = "查找满足 original-structure 为 'ThB5(P4/mmm)' 且 formation_energy 小于 0 和 img_number 为 0 的数据、且 ID 从小到大输出============="
# 构建查询条件
query = {
    "original-structure": "ThB5(P4/mmm)",
    "$and": [
        # {"structure.Superconductivity_related_properties.formation_energy": {"$lt": 0}},
        {"structure.Superconductivity_related_properties.img_number": 0}
    ]
}

pipeline = [
    {"$match": query},
    {"$addFields": {
        # 提取 "ID-123" 中的数值部分（假设格式为 "ID-数字"）
        "id_num": {
            "$toInt": {"$substr": ["$entry_id", 3, -1]}  # 从第3字符截取到末尾
        }
    }},
    {"$sort": {"id_num": 1}},  # 按数值升序排序
    {"$project": {"id_num": 0}}  # 隐藏临时字段
]

results = collection.aggregate(pipeline)
for doc in results:
    print(f"\nEntry ID: {doc['entry_id']}")
    print(f"Composition: {doc['composition']}")

    # 获取超导相关属性
    sc_props = doc['structure']['Superconductivity_related_properties']


    print("\nSuperconductivity Related Properties:")
    print(f"Formation Energy: {sc_props['formation_energy']}")
    print(f"Energy Above Hull: {sc_props['energy_above_hull']}")
    print(f"Image Number: {sc_props['img_number']}")
    print(f"Lambda Gamma: {sc_props['lambda_gamma']}")
    print(f"Low Three: {sc_props['low_three']}")

# 提取数据到列表
data = []
for doc in results:
    sc_props = doc['structure']['Superconductivity_related_properties']
    data.append({
        "Entry ID": doc['entry_id'],
        "Composition": doc['composition'],
        "Formation Energy": sc_props['formation_energy'],
        "Energy Above Hull": sc_props['energy_above_hull'],
        "Image Number": sc_props['img_number'],
        "Lambda Gamma": sc_props['lambda_gamma'],
        "Low Three": sc_props['low_three']
    })

# 创建 DataFrame 并保存为 CSV
df = pd.DataFrame(data)
df.to_csv("03-1-superconductivity_data.csv", index=False)  # 不保存行索引

print("数据已保存到 03-superconductivity_data.csv")

