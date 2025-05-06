# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 15:27:05 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
from pathlib import Path
import pymysql
import yaml
import time
import numpy as np


from utils.dirutils import load_path_config
from utils.logutils import FishStyleLogger
from utils.decorator_utils import run_by_thread


# %%
class DatabaseHandler:
    
    def __init__(self, mysql_name, log=None):
        self.mysql_name = mysql_name
        self.log = log
        
        self._init_logger()
        self._load_path_config()
        self._load_sql_config()
        
    def _init_logger(self):
        self.log = self.log or FishStyleLogger()
            
    def _load_path_config(self):
        file_path = Path(__file__).resolve()
        project_dir = file_path.parents[1]
        path_config = load_path_config(project_dir)
        
        self.sql_config_dir = Path(path_config['sql_config'])
        
    def _load_sql_config(self):
        file_path = self.sql_config_dir / f'{self.mysql_name}.yaml'
        with open(file_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        self.mysql_info = config['mysql']
        self.max_retries = config['max_retries']
        self.retry_delay = config['retry_delay']
        
    def connect(self):
        """尝试建立数据库连接，最多重试 max_retries 次"""
        retries = 0
        connection = None
        while retries < self.max_retries:
            try:
                # 尝试建立数据库连接
                connection = pymysql.connect(**self.mysql_info)
                if connection.open:
                    return connection
            except pymysql.MySQLError as e:
                retries += 1
                self.log.warning(f"Connection attempt {retries} failed: {e}")
                if retries < self.max_retries:
                    self.log.warning(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.log.error("Max retries reached, failed to connect to the database")
                    raise e  # 超过重试次数，抛出异常
        return connection

    def __enter__(self):
        """
        进入上下文时，自动建立数据库连接
        """
        self.connection = self.connect()
        if not self.connection:
            raise ConnectionError("Failed to establish database connection.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文时，关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def batch_insert_data(self, author, factor_category, factor_name, series, data_ts):
        """
        批量插入数据，使用 self.connection
        :param author: 共享的 author
        :param factor_category: 共享的 factor_category
        :param factor_name: 共享的 factor_name
        :param series: 一个 Pandas Series，索引为 symbol，值为因子值
        :param data_ts: 共享的时间戳
        """
        if not self.connection:
            raise ConnectionError("No active database connection. Use the context manager (with statement) to open a connection.")

        cursor = None
        try:
            # 过滤掉空值（None 或 NaN）
            filtered_series = series.replace([np.inf, -np.inf], np.nan).dropna()

            if filtered_series.empty:
                return  # 如果过滤后没有数据，则不进行插入

            # 执行批量插入操作
            cursor = self.connection.cursor()
            insert_query = """
            INSERT INTO factors_update (author, factor_category, factor_name, symbol, factor_value, data_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                factor_value = VALUES(factor_value),
                data_ts = VALUES(data_ts);
            """
            # 准备批量插入的数据
            data_to_insert = [
                (author, factor_category, factor_name, symbol, factor_value, data_ts)
                for symbol, factor_value in filtered_series.items()
            ]

            # 批量执行插入操作
            cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()

        except pymysql.MySQLError as e:
            self.log.error(f"Error while batch inserting or updating data: {e}")
            self.connection.rollback()  # 回滚事务

        finally:
            # 关闭游标
            if cursor:
                cursor.close()


# %%
# 示例：使用类插入数据，每次操作自动连接并断开
if __name__ == "__main__":
    db_handler = DatabaseHandler('factors_update')
    db_handler.insert_data(
        author="Author1", 
        factor_category="Category1", 
        factor_name="Factor1", 
        symbol="SYM123", 
        timestamp="2024-09-10 12:00:00", 
        factor_value=123.45
    )
