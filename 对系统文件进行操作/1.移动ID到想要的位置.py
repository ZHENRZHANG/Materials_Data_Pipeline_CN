import os
import shutil
from glob import glob

# 定义源文件夹路径列表
folder_paths = [
    r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\work (1)\work",
]

# 目标路径（移动到的目录）
target_dir = r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\DATA\origin-ThB5-all\B-221\02-origin-ThB5_12_34_221\S3-origin-ThB5-Alk+Tran_12_34_221"

# 确保目标目录存在（测试阶段也可以注释，仅打印路径）
os.makedirs(target_dir, exist_ok=True)

# 遍历每个源文件夹
for base_path in folder_paths:
    # 构建匹配模式：base_path下所有"*-*"子文件夹中的"ID-*"
    pattern = os.path.join(base_path, "*-*", "ID-*")
    # 匹配所有符合条件的路径
    matched_paths = glob(pattern)

    # 遍历匹配到的路径并移动
    for src_path in matched_paths:
        # 获取源路径的最后一级名称（用于目标路径）
        src_name = os.path.basename(src_path)
        dest_path = os.path.join(target_dir, src_name)

        try:
            # 移动路径（文件或文件夹均可）
            shutil.move(src_path, dest_path)
            print(f"成功移动：{src_path} -> {dest_path}")
        except Exception as e:
            print(f"移动失败 {src_path}：{str(e)}")
