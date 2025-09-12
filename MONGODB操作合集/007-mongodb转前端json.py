from pymongo import MongoClient
import pprint
import os
import json

# 读取配置文件
with open("config.json", "r") as f:
    config = json.load(f)["mongodb"]

client = MongoClient(config["uri"])
collection = client[config["db_name"]][config["collection_name"]]


def transform_document(document):
    doc_id = document.get("_id", "")

    composition = document.get("composition", {})
    # 修改这一行来处理当数量为1时不显示数字的情况
    name = "".join(f"{key}{value}" if value != 1 else f"{key}" for key, value in composition.items())
    role = name

    elements = list(composition.keys())
    if len(elements) == 3:
        if elements[1] == "B":
            # 处理 B 元素的情况
            elements = [elements[0], elements[2], elements[1]]
            role = "".join(
                f"{key}{composition[key]}" if composition[key] != 1 else key
                for key in elements
            ).replace(elements[0], "M", 1).replace(elements[1], "N", 1)

        else:
            role = name.replace(elements[0], "M", 1).replace(elements[1], "N", 1)

    elif len(elements) == 2:
        # Only one element exists, replace it with M
        role = name.replace(elements[0], "M", 1)

    image_path = "https://img.abclonal.com.cn/abclonal/Catalog/A11351/A11351_2.jpg?t=1691482739"

    lambda_gamma = document.get("Superconductivity_related_properties", {}).get("lambda_gamma", None)
    energy_relative_to_convex_hull = document.get("Superconductivity_related_properties", {}).get(
        "Energy_relative_to_convex_hull", None)
    posfile = document.get("Superconductivity_related_properties", {}).get("posfile", None)

    return {
        "id": doc_id,
        "name": name,
        "role": role,
        "image": image_path,
        "lambda_gamma": lambda_gamma,
        "energy_relative_to_convex_hull": energy_relative_to_convex_hull,
        "posfile": posfile
    }
