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


# %%
if __name__=='__main__':
    dir_path = "D:/crypto/prod/alpha/factors_update/project/factors/cgy/batch_241109/R1"  # 替换为你的目录路径
    output_file = "D:/crypto/prod/alpha/factors_update/project/factors/cgy/batch_241109/tasks.json"  # 输出 JSON 文件的路径
    
    generate_task_json_from_py_files(dir_path, output_file)

