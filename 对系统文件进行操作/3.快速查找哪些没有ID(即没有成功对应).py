import os
import shutil

# 设置路径
root_dir = r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\work_100-199"
target_move_dir = r"D:\School\SophomoreStudyMaterials\00MachineLearning\7.fuwuqi\work_Alk+poor_需要复原"

# 存储需要移动的目录
missing_id_dirs = []

print("正在扫描目录...")

# 遍历 root_dir 下的所有子目录
for entry in os.scandir(root_dir):
    if entry.is_dir():
        dir_name = entry.name

        # 判断目录名是否包含 '-'（即符合 */*-*/ 形式）
        if '-' in dir_name:
            target_path = entry.path
            has_id_subdir = False

            # 遍历该目录下的所有子项，检查是否有以 ID- 开头的【子目录】
            try:
                for sub_entry in os.scandir(target_path):
                    if sub_entry.is_dir() and sub_entry.name.startswith("ID-"):
                        has_id_subdir = True
                        break
            except PermissionError as e:
                print(f"⚠️ 无法访问 {target_path}：权限不足，跳过。")
                continue

            # 如果没有找到 ID- 开头的子目录，标记为需要移动
            if not has_id_subdir:
                missing_id_dirs.append(entry.path)

# 输出结果并移动
print("\n" + "=" * 60)
if missing_id_dirs:
    print("以下目录不包含以 'ID-' 开头的子目录，将被移动：")
    for dir_path in missing_id_dirs:
        dir_name = os.path.basename(dir_path)
        print(f"  {dir_name}")

    print(f"\n共 {len(missing_id_dirs)} 个目录将从:")
    print(f"  {root_dir}")
    print(f"移动到:")
    print(f"  {target_move_dir}")

    # 确认是否继续（可选，防止误操作）
    confirm = input("\n确认移动？(y/N): ").strip().lower()
    if confirm not in ('y', 'yes'):
        print("操作已取消。")
        exit()

    # 开始移动
    for dir_path in missing_id_dirs:
        dir_name = os.path.basename(dir_path)
        dest_path = os.path.join(target_move_dir, dir_name)

        # 如果目标位置已存在同名目录，跳过或重命名
        if os.path.exists(dest_path):
            print(f"⚠️ 目标已存在，跳过: {dir_name}")
            continue

        try:
            shutil.move(dir_path, dest_path)
            print(f"✅ 已移动: {dir_name}")
        except Exception as e:
            print(f"❌ 移动失败 {dir_name}: {str(e)}")
else:
    print("所有符合条件的目录都包含 'ID-' 子目录，无需移动。")

print("=" * 60)