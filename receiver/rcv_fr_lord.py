# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 10:34:59 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import zmq
from queue import Queue
from collections import defaultdict
import time


import receiver.cclob_pb2 as pb
from utils.data_parser import deserialize_pb, convert_to_lowercase
from utils.logutils import FishStyleLogger
from utils.decorator_utils import run_by_thread


# %%
_topic_mapping = {
    "CCBar": pb.CCBarMsg,
    "CCOrder": pb.CCOrderMsg,
    "CCTrade": pb.CCTradeMsg,
    "CCLevel": pb.CCLevelMsg,
    }


_default_address = "tcp://172.16.30.192:1602"


# %%
class LordMsgController:
    
    def __init__(self, topic_list, address=_default_address, log=None):
        self.topic_list = topic_list
        self.address = address
        self.log = log
        self._init_logger()
        self._set_socket()
        self._init_queue()
        self._init_topic_related()
        self._running = True
        
        self.log.success(f'Init LordMsgController with topics: {self.topic_list}')
        
    def _init_logger(self):
        self.log = self.log or FishStyleLogger()
        
    def _set_socket(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.address)
        
        for topic in self.topic_list:
            self.socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
            
    def _init_queue(self):
        self._queue_map = defaultdict(Queue)
            
    def _init_topic_related(self):
        self.topics = list(_topic_mapping.keys())
        assert all(topic in self.topics for topic in self.topic_list), (
            'The subscription topic contains unknown topic: {self.topic_list}')
    
    @run_by_thread
    def start(self):
        while self._running:
            # 使用 recv_multipart 接收多帧消息
            frames = self.socket.recv_multipart()
            if len(frames) != 2:
                self.log.error(f"Expected 2 frames but received {len(frames)}")
                continue
            
            # 第一帧是 topic，第二帧是 body 数据
            topic = frames[0].decode("utf-8")  # 假设 topic 是 UTF-8 编码
            data = frames[1]  # 第二帧是实际的数据
            if not topic:
                continue
            if not any(topic.startswith(prefix) for prefix in self.topics):
                self.log.error(f"Unknown topic: {topic}")
                continue
            
            for topic_prefix, pb_class in _topic_mapping.items():
                if topic.startswith(topic_prefix):
                    pb_msg = deserialize_pb(data, pb_class)
                    if pb_msg:
                        self._save_to_queue(topic_prefix, pb_msg)
                        
        self.socket.close()  # 关闭socket
        self.context.term()  # 销毁上下文
        self.log.success("ZMQ socket closed and context terminated.")
                        
    def stop(self):
        self._running = False

    def _save_to_queue(self, topic, pb_msg):
        self._queue_map[topic].put(pb_msg)
    
    def __getitem__(self, topic):
        assert topic in self.topics, f'Fetching unknown topic: {topic}'
        assert topic in self.topic_list, f"The topic '{topic}' has not been subscribed to."
        return self._queue_map[topic]
    
    
class LordWithFilter(LordMsgController):
    
    def _save_to_queue(self, topic, pb_msg):
        header = pb_msg.header
        symbol = convert_to_lowercase(header.symbol)
        if symbol.endswith('usdt'):
            self._queue_map[topic].put(pb_msg)
    
    
# %%
if __name__=='__main__':
    topic_list = ['CCLevel']
    lord = LordMsgController(topic_list)
    lord.start()
    time.sleep(20)
    lord.stop()