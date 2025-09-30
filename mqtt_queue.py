#!/usr/bin/env python3
import socket
import threading
import struct
import queue
import time

# 简单的 Topic -> list of client sockets
subscriptions = {}
subscriptions_lock = threading.Lock()

# 每个客户端的消息队列（socket -> Queue）
client_queues = {}
client_queues_lock = threading.Lock()

# 工具：读取固定长度
def recvall(sock, n):
    data = b''
    while len(data) < n:
        more = sock.recv(n - len(data))
        if not more:
            return None
        data += more
    return data

def handle_client(conn, addr):
    print(f"Client connected: {addr}")
    # 注册队列
    with client_queues_lock:
        client_queues[conn] = queue.Queue()

    def sender_loop():
        """不断从队列取消息发送给客户端"""
        q = client_queues[conn]
        while True:
            msg = q.get()
            if msg is None:
                break
            try:
                conn.sendall(msg)
            except Exception as e:
                print("Send error:", e)
                break

    t_sender = threading.Thread(target=sender_loop, daemon=True)
    t_sender.start()

    try:
        while True:
            # MQTT 固定头: 1 字节控制 + 剩余长度
            hdr = conn.recv(2)
            if not hdr:
                break
            ctrl, rem_len = struct.unpack("!BB", hdr)
            # 仅处理非常简单的单字节 rem_len 情况
            body = recvall(conn, rem_len)
            if body is None:
                break

            # 0x10 = CONNECT
            if (ctrl & 0xF0) == 0x10:
                # 忽略细节，直接回应 CONNACK
                # 固定回应: 0x20 0x02 0x00 0x00
                conn.sendall(b'\x20\x02\x00\x00')
            # 0x30 = PUBLISH (QoS0)
            elif (ctrl & 0xF0) == 0x30:
                # body: topic length + topic + payload
                topic_len = struct.unpack("!H", body[0:2])[0]
                topic = body[2:2+topic_len].decode()
                payload = body[2+topic_len:]
                # 构造 publish 消息给订阅者
                # rebuild publish packet: ctrl + rem + topic + payload
                send_topic = topic.encode()
                send_pkt = b'\x30' + struct.pack("!B", len(send_topic) + len(payload) + 2) + struct.pack("!H", len(send_topic)) + send_topic + payload

                # 找订阅这个 topic 的 clients
                with subscriptions_lock:
                    subs = subscriptions.get(topic, []).copy()

                for c in subs:
                    with client_queues_lock:
                        q = client_queues.get(c)
                    if q:
                        q.put(send_pkt)
            # 0x82 = SUBSCRIBE (如果 QoS1，这里简化处理)
            elif (ctrl & 0xF0) == 0x80:
                # body: packet id + topic length + topic + qos
                # 假定 rem_len足够
                pid = struct.unpack("!H", body[0:2])[0]
                topic_len = struct.unpack("!H", body[2:4])[0]
                topic = body[4:4+topic_len].decode()
                # qos byte 在 body[4+topic_len]
                # 加入订阅表
                with subscriptions_lock:
                    lst = subscriptions.setdefault(topic, [])
                    if conn not in lst:
                        lst.append(conn)
                # 发送 SUBACK: 0x90, len=3, packet id, qos 0
                suback = b'\x90\x03' + body[0:2] + b'\x00'
                conn.sendall(suback)
            # 0xE0 = DISCONNECT
            elif (ctrl & 0xF0) == 0xE0:
                break
            else:
                print("Unknown control:", hex(ctrl))
                break
    except Exception as e:
        print("Client handler exception:", e)
    finally:
        print("Client disconnected:", addr)
        # 清理
        with subscriptions_lock:
            for topic, lst in subscriptions.items():
                if conn in lst:
                    lst.remove(conn)
        with client_queues_lock:
            q = client_queues.pop(conn, None)
        if q:
            q.put(None)
        conn.close()

def start_server(host="0.0.0.0", port=1883):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(5)
    print(f"Broker listening on {host}:{port}")
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

if __name__ == "__main__":
    start_server()
