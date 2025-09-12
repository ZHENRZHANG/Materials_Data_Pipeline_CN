from pymongo import MongoClient
import numpy as np
import json


def parse_poscar(poscar_path):
    with open(poscar_path, 'r') as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    # 解析元素和数量
    elements = lines[5].split()
    counts = list(map(int, lines[6].split()))
    composition = dict(zip(elements, counts))

    # 解析晶格常数
    lattice = []
    for line in lines[2:5]:
        lattice.append([float(x) for x in line.split()[:3]])

    # 解析原子位置
    sites = []
    for i, line in enumerate(lines[8:8+sum(counts)]):
        parts = line.split()
        abc = list(map(float, parts[:3]))
        label = elements[np.searchsorted(np.cumsum(counts), i+1)]
        sites.append({
            'abc': abc,
            'label': label,
            'species': [{'element': label, 'occu': 1}]
        })

    return {
        'composition': composition,
        'lattice': {'matrix': lattice},
        'sites': sites
    }

def build_query(poscar_data):
    # 构建composition查询
    composition_query = {'composition': poscar_data['composition']}

    # 构建lattice查询 - 精确匹配晶格矩阵
    lattice_query = {
        'structure.lattice.matrix': poscar_data['lattice']['matrix']
    }

    # 构建sites查询 - 匹配关键原子位置（允许微小浮点差异）
    sites_query = {
        '$and': [
            {'structure.sites': {
                '$elemMatch': {
                    'label': site['label'],
                    '$and': [
                        {'abc.0': {'$gte': site['abc'][0] - 1e-6, '$lte': site['abc'][0] + 1e-6}},
                        {'abc.1': {'$gte': site['abc'][1] - 1e-6, '$lte': site['abc'][1] + 1e-6}},
                        {'abc.2': {'$gte': site['abc'][2] - 1e-6, '$lte': site['abc'][2] + 1e-6}}
                    ]
                }
            }}
            for site in poscar_data['sites'][:]
        ]
    }

    return {**composition_query, **lattice_query, **sites_query}

# 使用示例
if __name__ == "__main__":
    # 连接到 MongoDB

    # 读取配置文件
    with open("config.json", "r") as f:
        config = json.load(f)["mongodb"]

    client = MongoClient(config["uri"])
    collection = client[config["db_name"]][config["collection_name"]]

    # 解析POSCAR文件
    poscar_data = parse_poscar('010-POSCAR')  # 替换为你的POSCAR文件路径

    # 构建查询
    query = build_query(poscar_data)

    # 执行查询
    try:
        results = collection.find(query)

        # 输出结果
        count = 0
        for doc in results:
            count += 1
            print(f"Found match ({count}): {doc.get('entry_id', 'N/A')}")
            props = doc.get('structure', {}).get('Superconductivity_related_properties', {})
            print(f"Formation energy: {props.get('formation_energy', 'N/A')}")
            print(f"Energy above hull: {props.get('energy_above_hull', 'N/A')}")
            print("-" * 50)

        if count == 0:
            print("No matching structures found.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")