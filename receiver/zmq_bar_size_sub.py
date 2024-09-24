# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 14:11:46 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
import zmq
import time
from google.protobuf.json_format import MessageToJson
from receiver.size_msg_bar_pb2 import CCBarSizeMsg

def main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://172.16.30.44:15005")  # 请根据实际情况修改地址和端口
    socket.setsockopt_string(zmq.SUBSCRIBE, "")  # 订阅所有主题

    print("已连接到ZMQ发布者并订阅所有主题")
    print("等待接收消息...")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while True:
        try:
            socks = dict(poller.poll(timeout=1000))  # 1秒超时
            if socket in socks and socks[socket] == zmq.POLLIN:
                topic = socket.recv_string()
                message = socket.recv()
                
                bar_size_msg = CCBarSizeMsg()
                bar_size_msg.ParseFromString(message)
                
                json_message = MessageToJson(bar_size_msg)
                
                print(f"接收到主题: {topic}")
                print(f"消息内容:\n{json_message}")
                print("-" * 50)
            else:
                print("等待消息中...")
            
        except KeyboardInterrupt:
            print("程序终止")
            break
        except Exception as e:
            print(f"发生错误: {e}")
        
        time.sleep(0.1)  # 短暂休眠以避免CPU使用率过高

if __name__ == "__main__":
    main()
