# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 16:24:43 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import os
import json
import pandas as pd
from pathlib import Path


# %%
def generate_task_json_from_py_files(dir_path, output_file):
    """
    读取目录下的所有 .py 文件（排除 __init__.py），提取文件名（不包含后缀），并将列表存为 JSON 文件。
    
    :param dir_path: 要读取的目录路径
    :param output_file: 输出的 JSON 文件路径
    """
    try:
        # 获取目录中的所有文件
        files = os.listdir(dir_path)
        
        # 筛选出 .py 文件（排除 __init__.py）并去掉后缀
        tasks = [
            os.path.splitext(file)[0]
            for file in files
            if file.endswith('.py') and file != "__init__.py"
        ]
        
        # 生成 JSON 数据
        json_data = {"tasks": tasks}
        
        # 写入到指定的输出文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"成功生成 JSON 文件: {output_file}")
        print(json_data)
    except Exception as e:
        print(f"生成 JSON 文件失败: {e}")
        
        
def generate_task_json_from_clusters(cluster_paths, author, output_file):
    tasks = []
    for cl_path in cluster_paths:
        cluster_info = pd.read_csv(cl_path)
        target_cluster_info = cluster_info[cluster_info['tag_name']=='cgy_ver1109']
        selected_names = [name.split('_')[0] for name in target_cluster_info['factor']
                          if 'P0.4' in name] # !!!
        tasks.extend(selected_names)
    tasks = list(set(tasks))
    
    # 生成 JSON 数据
    json_data = {"tasks": tasks}
    
    # 写入到指定的输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    
    print(f"成功生成 JSON 文件: {output_file}")
    print(json_data)


# %%
if __name__=='__main__':
# =============================================================================
#     dir_path = "D:/crypto/prod/alpha/factors_update/project/factors/cgy/batch_241109/R1"  # 替换为你的目录路径
#     output_file = "D:/crypto/prod/alpha/factors_update/project/factors/cgy/batch_241109/tasks.json"  # 输出 JSON 文件的路径
#     
#     generate_task_json_from_py_files(dir_path, output_file)
# =============================================================================
    cluster_paths = [
        # '/mnt/Data/xintang/multi_factor/factor_test_by_alpha/results/cluster/agg_241113_double3m/cluster_info_221201_241201.csv',
        # '/mnt/Data/xintang/multi_factor/factor_test_by_alpha/results/cluster/agg_241114_zxt_cgy_double3m/cluster_info_221201_241201.csv',
        # '/mnt/Data/xintang/multi_factor/factor_test_by_alpha/results/cluster/agg_241227_cgy_zxt_double3m/cluster_info_230101_250101.csv',
        '/mnt/Data/xintang/multi_factor/factor_test_by_alpha/results/cluster/agg_250127_cgy_zxt_double3m/cluster_info_230201_250201.csv',
        ]
    author = 'cgy_ver1109'
    output_dir = Path("/home/xintang/crypto/prod/alpha/factors_update/project/factors/cgy/batch_241109_p0.4")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "tasks.json" 
    generate_task_json_from_clusters(cluster_paths, author, output_file)
