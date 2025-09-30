#!/usr/bin/env python3
import asyncio
import struct
from collections import defaultdict

class MQTTBroker:
    def __init__(self):
        # topic -> set of writer
        self.subscriptions = defaultdict(set)
        self.lock = asyncio.Lock()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        print("Client connected:", peer)
        try:
            while True:
                # 读固定头两字节
                hdr = await reader.readexactly(2)
                ctrl, rem_len = struct.unpack("!BB", hdr)
                body = await reader.readexactly(rem_len)
                # CONNECT
                if (ctrl & 0xF0) == 0x10:
                    # 忽略认证、协议名等，直接回 CONNACK
                    writer.write(b'\x20\x02\x00\x00')
                    await writer.drain()
                # PUBLISH QoS0
                elif (ctrl & 0xF0) == 0x30:
                    topic_len = struct.unpack("!H", body[0:2])[0]
                    topic = body[2:2+topic_len].decode()
                    payload = body[2+topic_len:]
                    # 构建 publish 包
                    pub_payload = topic.encode()
                    # 新的 remaining length = 2 + len(topic) + len(payload)
                    new_rem = 2 + len(pub_payload) + len(payload)
                    pkt = b'\x30' + struct.pack("!B", new_rem) + struct.pack("!H", len(pub_payload)) + pub_payload + payload
                    # 转发给所有订阅者
                    async with self.lock:
                        subs = list(self.subscriptions.get(topic, set()))
                    for w in subs:
                        try:
                            w.write(pkt)
                            await w.drain()
                        except Exception as e:
                            print("Forward error:", e)
                # SUBSCRIBE (假设只有一个主题)
                elif (ctrl & 0xF0) == 0x80:
                    # body: packet id + topic length + topic + qos
                    pid = struct.unpack("!H", body[0:2])[0]
                    tlen = struct.unpack("!H", body[2:4])[0]
                    topic = body[4:4+tlen].decode()
                    async with self.lock:
                        self.subscriptions[topic].add(writer)
                    # send SUBACK
                    suback = b'\x90\x03' + body[0:2] + b'\x00'
                    writer.write(suback)
                    await writer.drain()
                # DISCONNECT
                elif (ctrl & 0xF0) == 0xE0:
                    break
                else:
                    print("Unknown ctrl:", hex(ctrl))
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            print("Client error:", e)
        finally:
            print("Client disconnected:", peer)
            # 清理订阅
            async with self.lock:
                for topic, subs in self.subscriptions.items():
                    subs.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except:
                pass

    async def run(self, host="0.0.0.0", port=1883):
        server = await asyncio.start_server(self.handle_client, host, port)
        print(f"Broker running on {host}:{port}")
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    broker = MQTTBroker()
    try:
        asyncio.run(broker.run())
    except KeyboardInterrupt:
        print("Broker shutting down")
