import os

# 目标文件夹路径
a = r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\DATA\origin-ThB5-Alk+Alk-123-4"

# 遍历文件夹下所有文件
# 只查找直接子文件夹（不递归）
for item in os.listdir(a):
    item_path = os.path.join(a, item)
    if os.path.isdir(item_path) and "4B20" in item:
        print(item_path)