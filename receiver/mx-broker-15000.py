#coding:utf-8

"""
https://gist.github.com/minrk/4667957
"""

import os
import string
import sys
import time
from random import randint
import zmq
#import fire

def init_keepalive(sock):
    sock.setsockopt(zmq.TCP_KEEPALIVE,1)
    sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE,120)
    sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL,1)
    sock.set_hwm(0)

MX_SUB_ADDR = "tcp://127.0.0.1:1608"
MX_PUB_ADDR = "tcp://127.0.0.1:1609"



ctx = zmq.Context()

# https://blog.csdn.net/weixin_43214364/article/details/82811095

# xsub (bind ) , xpub(bind)
def run_spbb(sub_addr = MX_SUB_ADDR ,pub_addr=MX_PUB_ADDR,**kvs):
    xpub_url = pub_addr
    xsub_url = sub_addr

    print('xSub bind:', xsub_url,'waiting for  incoming..')
    print('xPub bind:', xpub_url,'waiting for  incoming..')


    xpub = ctx.socket(zmq.XPUB)
    init_keepalive(xpub)
    xpub.bind(xpub_url)
    xsub = ctx.socket(zmq.XSUB)
    init_keepalive(xsub)
    xsub.bind(xsub_url)

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)
    while True:
        events = dict(poller.poll(1000))
        if xpub in events:
            message = xpub.recv_multipart()
            print("[BROKER] xpub. subscription message: %r" % message[0])
            xsub.send_multipart(message)
        if xsub in events:
            message = xsub.recv_multipart()
            print("publishing message: %r" % message)
            xpub.send_multipart(message)

# https://blog.csdn.net/qq_41453285/article/details/106888222
# xsub (connect) , xpub ( bind )
def run_spcb(sub_addr = MX_SUB_ADDR,pub_addr=MX_PUB_ADDR,**kvs):
    xpub_url = pub_addr
    xsub_url = sub_addr

    print('xSub:','connect to :',xsub_url)
    print('xPub bind:', xpub_url,'waiting for incoming..')


    xpub = ctx.socket(zmq.XPUB)
    init_keepalive(xpub)
    xpub.bind(xpub_url)
    xsub = ctx.socket(zmq.XSUB)
    init_keepalive(xsub)
    xsub.connect(xsub_url)

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)
    while True:
        events = dict(poller.poll(1000))
        if xpub in events:
            message = xpub.recv_multipart()
            print("[BROKER] subscription message: %r" % message[0])
            xsub.send_multipart(message)
        if xsub in events:
            message = xsub.recv_multipart()
            print("publishing message: %r" % message)
            xpub.send_multipart(message)

def run_mspcb(sub_addr = MX_SUB_ADDR,pub_addr=MX_PUB_ADDR,**kvs):
    """多上级源mx接入， 主动连接多个上游 mx，转发消息到单个下游mx
        python -m elabs.utils.mx-client run_mspcb
                --sub_addr=tcp://x.x.x.x:15901,tcp//x.x.x.x:15902
                --pub_addr=tcp://*:16001
    """
    xpub_url = pub_addr
    xsub_url = sub_addr

    print('xSub:','connect to :',xsub_url)
    print('xPub bind:', xpub_url,'waiting for incoming..')


    xpub = ctx.socket(zmq.XPUB)
    init_keepalive(xpub)
    xpub.bind(xpub_url)

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)


    xsub_socks = []
    xsub_urls  = xsub_url.split(',')

    for xsub_url in xsub_urls:
        xsub = ctx.socket(zmq.XSUB)
        init_keepalive(xsub)
        xsub_socks.append( xsub )
        xsub.connect(xsub_url)
        poller.register(xsub, zmq.POLLIN)

    while True:
        events = dict(poller.poll(1000))
        if xpub in events:
            message = xpub.recv_multipart()
            print("[BROKER] subscription message: %r" % message[0])
            xsub.send_multipart(message)

        for xsub in xsub_socks:
            if xsub in events:
                message = xsub.recv_multipart()
                print("publishing message: %r" % message)
                xpub.send_multipart(message)

def run_mspcb(sub_addr = MX_SUB_ADDR,pub_addr=MX_PUB_ADDR,**kvs):
    """多上级源mx接入， 主动连接多个上游 mx，转发消息到单个下游mx
        python -m elabs.utils.mx-client run_mspcb
                --sub_addr=tcp://x.x.x.x:15901,tcp//x.x.x.x:15902
                --pub_addr=tcp://*:16001
    """
    xpub_url = pub_addr
    xsub_url = sub_addr

    print('xSub:','connect to :',xsub_url)
    print('xPub bind:', xpub_url,'waiting for incoming..')


    xpub = ctx.socket(zmq.PUB)
    init_keepalive(xpub)
    xpub.bind(xpub_url)

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)


    xsub_socks = []
    xsub_urls  = xsub_url.split(',')

    for xsub_url in xsub_urls:
        xsub = ctx.socket(zmq.SUB)
        init_keepalive(xsub)
        xsub_socks.append( xsub )
#         xsub.setsockopt(zmq.SUBSCRIBE, for_subscribe_address('0001') )
        xsub.setsockopt(zmq.SUBSCRIBE, b'' )
        xsub.connect(xsub_url)
        poller.register(xsub, zmq.POLLIN)

    while True:
        events = dict(poller.poll(1000))
#         if xpub in events:
#             message = xpub.recv_multipart()
#             print("[BROKER] subscription message: %r" % message[0])
#             xsub.send_multipart(message)

        for xsub in xsub_socks:
            if xsub in events:
                message = xsub.recv_multipart()
                # print("publishing message: %r" % message)
                xpub.send_multipart(message)


if __name__ == '__main__':
    #fire.Fire()
    run_spbb('tcp://172.16.30.97:15000','tcp://172.16.30.97:15001')
