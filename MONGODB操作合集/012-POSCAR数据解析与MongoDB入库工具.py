"""
🌟 脚本名称：POSCAR数据解析与MongoDB入库工具

📌 功能概述：
本脚本用于自动化处理材料科学中的 POSCAR/CONTCAR 文件，提取晶体结构信息（晶格、原子坐标、化学式等），
并结合外部超导相关属性数据（如 λ、Tc、形成能等），将结构化数据批量导入 MongoDB 数据库。
同时支持去重更新、文件归档与附件（图像、数据文件）自动复制，适用于高通量计算数据管理。

🔧 核心功能：
1. ✅ 自动解析文件路径中的化学式（支持复杂命名规则）
2. ✅ 读取 POSCAR/CONTCAR 文件，构建标准晶体结构数据（lattice + sites）
3. ✅ 关联外部属性数据（来自 '合并后的数据.txt'，支持多列格式）
4. ✅ 智能生成唯一 entry_id（如 ID-1, ID-2...），支持断点续传
5. ✅ 基于结构内容（lattice 和 sites）判断是否已存在，实现 upsert（存在则更新，否则插入）
6. ✅ 自动创建以 entry_id 命名的文件夹，归档结构文件与相关图表（gamma-figsum.png, omega.dat）
7. ✅ 支持从 config.json 读取数据库配置，避免硬编码，提升安全性与可移植性

📁 输入要求：
- POSCAR/CONTCAR 文件路径结构示例：
    D:/.../work/Fe2Se/CONTCAR
    （文件夹名应包含化学式，如 Fe2Se）
- 外部属性数据文件：
    "合并后的数据.txt"，位于每个工作目录下，格式为 TSV（制表符分隔），支持以下列：
    化学式    formation_e    E_d    lambda    img_nu    low_three
    或
    化学式    formation_e    E_d


📦 输出结果：
- 数据写入 MongoDB 指定集合
- 每个材料生成独立文件夹（如 ID-100/），包含：
    - {化学式}.vasp（结构文件副本）
    - gamma-figsum.png（超导谱图）
    - omega.dat（声子频率数据）

❗ 注意事项：
- 若化学式无法从路径中解析，会记录到 '012-poscar2mondodb_wrong' 并跳过
- 若数据库中已存在相同结构，将保留原 entry_id 并强制更新内容
- 请确保 MongoDB 具备读写权限，并提前备份重要数据

🚀 使用建议：
1. 修改 folder_paths 为你的实际数据目录
2. 确保 '合并后的数据.txt' 文件存在且格式正确
3. 首次运行建议先备份数据库
4. 可通过注释 start_count 相关逻辑强制从 ID-1 开始

📅 作者：张圳锐
"""
from pymongo import MongoClient
import os
import shutil
import json
import ast
from datetime import datetime, timezone
import re
import json

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]



# ===============================================
def extract_formula_from_path(file_path):
    """
    从文件路径中提取化学式的高级匹配方法

    参数:
        file_path: 文件路径字符串

    返回:
        提取到的化学式字符串，如果无法提取则返回None
    """
    # 改进后的正则表达式，允许元素后没有数字
    pattern = r'([A-Z][a-z]?(?:\d*[A-Z][a-z]?\d*)*)'

    # 从路径中提取可能的化学式部分
    matches = re.findall(pattern, file_path)

    if matches:
        # 优先取较长的匹配（假设化学式会更长）
        matches.sort(key=len, reverse=True)

        # 验证是否是合理的化学式
        for candidate in matches:
            # 检查是否包含至少两个元素
            elements = re.findall(r'([A-Z][a-z]?)', candidate)
            if len(elements) >= 2:
                # 移除对数字的强制要求，但仍保留对有效元素组合的检查
                # 检查是否只包含元素符号和数字
                if re.fullmatch(r'([A-Z][a-z]?\d*)+', candidate):
                    return candidate

    return None


def parse_poscar_composition(path, current_count):
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 晶格矩阵
    lattice_matrix = []
    for i in range(2, 5):
        lattice_matrix.append([float(x) for x in lines[i].split()])

    # 第6行为元素名行
    elements = lines[5].split()
    counts = list(map(int, lines[6].split()))
    composition = dict(zip(elements, counts))

    # 原子坐标
    sites = []
    index = 8

    # 遍历每种元素及其对应的数量
    for element, count in zip(elements, counts):
        for _ in range(count):
            coords = list(map(float, lines[index].split()))
            sites.append({
                "species": [{"element": element, "occu": 1}],
                "abc": coords,
                "label": element
            })
            index += 1  # 更新索引以指向下一个原子的位置

    #  超导相关
    #  这里需要你修改
    #material_name = path.split("-")[-4]  ## 需要高级配符
    material_name = extract_formula_from_path(path)

    if chem_dict.get(material_name):
        lambda_gamma = chem_dict.get(material_name, {}).get('lambda', None)
        energy_relative_to_convex_hull = chem_dict.get(material_name, {}).get('E_d (eV/atom)', None)
        img_number = chem_dict.get(material_name, {}).get('img_nu', None)
        formation_energy = chem_dict.get(material_name, {}).get('formation_e (eV/atom)', None)
        a = chem_dict.get(material_name, {}).get('low_three', None)

        if a is None:
            low_three = None
        else:
            # 只有 a 不是 None 时，才进行字符串操作
            cleaned_str = a.replace('\n', '').replace(' ', '')
            low_three = ast.literal_eval(cleaned_str)
        current_utc_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M%z")

        result = {
            "entry_id": f"ID-{current_count}",
            "composition": composition,
            "original-structure": "ThB5(P4/mmm)",
            "datatime": current_utc_time,
            "structure": {
                "lattice": {
                    "matrix": lattice_matrix,
                },
                "sites": sites,
                "Superconductivity_related_properties": {
                    "lambda_gamma": lambda_gamma if 'lambda_gamma' in locals() and lambda_gamma is not None else None,
                    "energy_above_hull": energy_relative_to_convex_hull if 'energy_relative_to_convex_hull' in locals() and energy_relative_to_convex_hull is not None else None,
                    "img_number": img_number if 'img_number' in locals() and img_number is not None else None,
                    "formation_energy": formation_energy if 'formation_energy' in locals() and formation_energy is not None else None,
                    "low_three": low_three if 'low_three' in locals() and low_three is not None else None
                }
            }
        }
        return result
    else:
        print(material_name + " 该化学式不在字典中")
        with open('012-poscar2mondodb_wrong.txt', 'a', encoding='utf-8') as f:
            f.write(path + '\n')
        return "error"


# 获取当前最大的entry_id数值，用于递增计数
def get_max_entry_id(collection):
    # 按数字部分降序排序，取第一条（修正排序逻辑）
    pipeline = [
        {
            "$addFields": {
                "entry_num": {
                    "$toInt": {
                        "$arrayElemAt": [{"$split": ["$entry_id", "-"]}, 1]
                    }
                }
            }
        },
        {"$sort": {"entry_num": -1}},
        {"$limit": 1}
    ]
    max_docs = list(collection.aggregate(pipeline))
    if not max_docs or "entry_id" not in max_docs[0]:
        return 0  # 如果没有数据，从1开始
    # 提取数字部分（例如从"ID-5"中提取5）
    return int(max_docs[0]["entry_id"].split("-")[-1])


# 获取起始计数
start_count = get_max_entry_id(collection) + 1  # 下一个要分配的ID
# start_count = 1  # 如果需要从1开始计数，可以将此行取消注释
current_count = start_count  # 当前计数

# 定义文件夹路径列表
folder_paths = [
    r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\work\work",
]
for folder_path in folder_paths:
    target_file_path = os.path.join(folder_path, '合并后的数据.txt')
    chem_dict = {}
    # 检查文件是否存在（可选）
    if os.path.exists(target_file_path):
        with open(target_file_path, 'r') as f1:
            lines = f1.readlines()
            # 跳过标题行（假设第一行是标题）
            for line in lines[1:]:
                # 去除行首尾的空白字符
                line = line.strip()
                # 分割行中的数据
                parts = [part.strip() for part in line.split('\t') if part.strip()]

                # 确保行中有足够的数据
                if len(parts) >= 6:
                    chem, formation_e, e_d, lambda_val, img_nu, low_three = parts
                    # print(chem)
                    chem_dict[chem] = {
                        'lambda': float(lambda_val),
                        'E_d (eV/atom)': float(e_d),
                        'formation_e (eV/atom)': float(formation_e),
                        'img_nu': int(img_nu),
                        'low_three': low_three
                    }

                if len(parts) == 3:
                    chem, formation_e, e_d = parts
                    chem = chem.split('-')[1]  # 提取化学式部分
                    chem_dict[chem] = {
                        'formation_e (eV/atom)': float(formation_e),
                        'E_d (eV/atom)': float(e_d)
                    }
    print(folder_path)
    for file_path in os.listdir(folder_path):
        if "-" in file_path:
            # formula = file_path.split("-")[1]  # 提取化学式

            formula = extract_formula_from_path(file_path)
            gamma_png = os.path.join(folder_path, file_path, "gamma-figsum.png")
            omega = os.path.join(folder_path, file_path, "omega.dat")

            file_path = os.path.join(folder_path, file_path, "CONTCAR")

            print(file_path, current_count)
            # 生成包含 entry_id 的结果
            if parse_poscar_composition(file_path, current_count) != "error":
                result = parse_poscar_composition(file_path, current_count)
                print(f"Inserting document with entry_id: {result['entry_id']}")
                # 唯一判断表示
                lattice = result["structure"]["lattice"]
                sites = result["structure"]["sites"]

                # 准备要替换的数据，注意：不能包含_id字段（如果有的话）
                data_to_replace = result.copy()
                if '_id' in data_to_replace:
                    del data_to_replace['_id']  # 移除_id字段，因为MongoDB不允许替换_id

                query = {"structure.lattice": lattice
                    , "structure.sites": sites}
                existing_doc = collection.find_one(query)

                if existing_doc:
                    print(existing_doc)
                print("*****" * 10)

                if existing_doc:
                    # 保留原有的entry_id
                    data_to_replace['entry_id'] = existing_doc['entry_id']

                # 使用replace_one并设置upsert=True
                replace_result = collection.replace_one(
                    query,
                    data_to_replace,
                    upsert=True
                )

                entry_id = data_to_replace['entry_id']

                if replace_result.upserted_id is not None:
                    print(f"{current_count} 新文档已插入: {file_path} (ID: {entry_id})")
                    current_count += 1
                else:
                    print(f"{current_count} 结构已存在!!!!!!!!!!!!!!!!!!，已强制替换文档: {file_path} (ID: {entry_id})")
                    print("!!!!!!!!!!" * 20)

                base_dir = os.path.dirname(file_path)  # 获取文件所在目录
                target_dir = os.path.join(base_dir, entry_id)  # 目标文件夹路径
                os.makedirs(target_dir, exist_ok=True)  # 创建文件夹（如果不存在）

                target_file = os.path.join(target_dir, f"{formula}.vasp")

                # 移动文件
                try:
                    shutil.copy2(file_path, target_file)
                except FileNotFoundError:
                    print(f"文件不存在，跳过: {file_path}")
                except Exception as e:
                    print(f"复制文件时发生未知错误: {file_path} -> {str(e)}")

                try:
                    shutil.copy2(gamma_png, target_dir)
                except FileNotFoundError:
                    print(f"文件不存在，跳过: {gamma_png}")
                except Exception as e:
                    print(f"复制文件时发生未知错误: {gamma_png} -> {str(e)}")

                try:
                    shutil.copy2(omega, target_dir)
                except FileNotFoundError:
                    print(f"文件不存在，跳过: {omega}")
                except Exception as e:
                    print(f"复制文件时发生未知错误: {omega} -> {str(e)}")
