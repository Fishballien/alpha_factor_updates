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
from collections import namedtuple
import time
from pathlib import Path
import sys


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from receiver.cclob_pb2 import CCBarMsg, CCOrderMsg, CCTradeMsg, CCLevelMsg
from receiver.size_msg_bar_pb2 import CCBarSizeMsg
from utils.data_parser import deserialize_pb, convert_to_lowercase
from utils.logutils import FishStyleLogger
from utils.decorator_utils import run_by_thread
from receiver.msg_handler import handler_msg_fr_lord, handler_msg_fr_cluster


# %%
Topic = namedtuple('Topic', ['name', 'address', 'handler'])


_topic_mapping = {
    "CCBar": Topic(name="CCBar", address="tcp://172.16.30.192:1602", handler=CCBarMsg),
    "CCSizeBar": Topic(name="CCSizeBar", address="tcp://172.16.30.192:15005", handler=CCBarSizeMsg),
    "CCOrder": Topic(name="CCOrder", address="tcp://172.16.30.192:1602", handler=CCOrderMsg),
    "CCTrade": Topic(name="CCTrade", address="tcp://172.16.30.192:1602", handler=CCTradeMsg),
    "CCLevel": Topic(name="CCLevel", address="tcp://172.16.30.192:1602", handler=CCLevelMsg),
    "CCRngLevel": Topic(name="CCRngLevel", address="tcp://172.16.30.192:1602", handler=CCLevelMsg),
    }


_default_address = "tcp://172.16.30.192:1602"


_address_handler = {
    "tcp://172.16.30.192:1602": handler_msg_fr_lord,
    "tcp://172.16.30.192:15005": handler_msg_fr_cluster,
    }


_address_topics = {
    "tcp://172.16.30.192:1602": ["CCBar", "CCOrder", "CCTrade", "CCLevel", "CCRngLevel"],
    "tcp://172.16.30.192:15005": ["CCSizeBar"],
    }


# %%
class LordMsgController:
    
    def __init__(self, topic_list, address=_default_address, log=None):
        self.topic_list = topic_list
        self.log = log
        self._set_address_list(address)
        self._init_logger()
        self._init_topic_related()
        self._set_socket()
        self._init_queue()
        self._running = True
        
        self.log.success(f'Init LordMsgController with topics: {self.topic_list}')

    def _set_address_list(self, address):
        assert isinstance(address, (str, list))
        self.address_list = [address] if isinstance(address, str) else address
        
    def _init_logger(self):
        self.log = self.log or FishStyleLogger()
        
    def _init_topic_related(self):
        self.topics = list(_topic_mapping.keys())
        assert all(topic in self.topics for topic in self.topic_list), (
            f'The subscription topic contains unknown topic: {self.topic_list}')
        
    def _set_socket(self):
        self.context = zmq.Context()
        self.subscribers = {}
        
        for address in self.address_list:
            self.subscribers[address] = self.context.socket(zmq.SUB)
            self.subscribers[address].connect(address)
        
        for topic in self.topic_list:
            target_address = _topic_mapping[topic].address
            self.subscribers[target_address].setsockopt_string(zmq.SUBSCRIBE, topic) #.encode('utf-8'))
            
        self.poller = zmq.Poller()
        
        for address in self.address_list:
            self.poller.register(self.subscribers[address], zmq.POLLIN)
            
    def _init_queue(self):
        self._queue_map = defaultdict(Queue)

    @run_by_thread
    def start(self):
        while self._running:
            
            events = dict(self.poller.poll())
            
            for address, socket in self.subscribers.items():
                if not socket in events:
                    continue
                rcv = _address_handler[address](socket, self.log, _address_topics[address])
                if rcv:
                    topic_name, data = rcv
                    self._process_one_msg(topic_name, data)
        
        for socket in self.subscribers.values():
            socket.close()  # 关闭socket
        self.context.term()  # 销毁上下文
        self.log.success("ZMQ socket closed and context terminated.")
        
    def _process_one_msg(self, topic_name, data):
        for topic_prefix, topic in _topic_mapping.items():
            pb_class = topic.handler
            if topic_name.startswith(topic_prefix):
                pb_msg = deserialize_pb(data, pb_class)
                if pb_msg:
                    # print(pb_msg)
                    self._save_to_queue(topic_prefix, pb_msg)
                        
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
    topic_list = ['CCLevel', "CCSizeBar"]
    address = ["tcp://172.16.30.192:1602", "tcp://172.16.30.192:15005"]
    lord = LordMsgController(topic_list, address)
    lord.start()
    time.sleep(120)
    lord.stop()