import os

# 目标目录路径
target_dir = r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\DATA\origin-ThB5-all\B-221\03-origin-ThB5_13_24_221"

# 递归查找所有 ID-* 目录
id_dirs = []
for root, dirs, files in os.walk(target_dir):
    for dir_name in dirs:
        if dir_name.startswith("ID-"):
            full_path = os.path.join(root, dir_name)
            id_dirs.append(full_path)

# 输出结果
id_dirs_names = [dir_name for root, dirs, files in os.walk(target_dir)
                for dir_name in dirs if dir_name.startswith("ID-")]
print(id_dirs_names)