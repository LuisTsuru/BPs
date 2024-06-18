"""
IoT Data Hub Implementation
Author: Professor Afonso Miguel
Date: May 25, 2024
Versio: 0.1

This script implements an IoT Data Hub that connects to a Wi-Fi network and communicates
with an MQTT broker to publish and subscribe to topics.
"""

import sys
import time
import _thread
import random

# Main class for the IoT Data Hub
class IoTDataHub:
    def __init__(self,
                ssid,
                password,
                mqtt_account_id,
                callback_func=None,
                verbose=False,
                mqtt_client_id=None):
        # Initialization parameters
        self.mqtt_client = None
        self.station = None
        self.ssid = ssid
        self.password = password
        self.callback = callback_func
        self.abort = False
        self.verbose = verbose

        # MQTT Credentials
        if mqtt_client_id is None:
            self.mqtt_client_id = self.__get_random_device_id(20)
        else:
            self.mqtt_client_id = mqtt_client_id
        if self.verbose:
            print(f"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\nClient ID: {self.mqtt_client_id}")
        self.mqtt_account_id = mqtt_account_id
        self.mqtt_server = "broker.mqttdashboard.com"
        self.mqtt_port = 1883
        self.mqtt_user = ""
        self.mqtt_password = ""

        # Connect to Wi-Fi
        if self.verbose:
            print(f"Connecting to Wi-Fi {self.ssid}")
        self.station = self.__wifi_connect()
        if not self.station.isconnected():
            print(f"Failed to connect to network {self.ssid}!")
            sys.exit(1)

        # Connect to MQTT Broker
        if self.verbose:
            print(f"Connecting to MQTT Broker {self.mqtt_server}")
        self.mqtt_client = MQTTClient(self.mqtt_client_id,
                                      self.mqtt_server,
                                      self.mqtt_port,
                                      self.mqtt_user,
                                      self.mqtt_password)
        self.mqtt_client.connect()
        if self.callback is not None:
            self.mqtt_client.set_callback(self.message_received)
            _thread.start_new_thread(self.check_message_loop, ())

    # Loop to check for MQTT messages
    def check_message_loop(self):
        while True:
            self.mqtt_client.check_msg()
            if self.abort:
                break
            time.sleep(.5)

    # Method called when a message is received
    def message_received(self, topic, msg):
        topic_list = topic.decode().split('/')
        self.callback('/'.join(topic_list[1:]), msg.decode())

    # Method to disconnect from MQTT and Wi-Fi
    def __del__(self):
        self.disconnect()

    def disconnect(self):
        self.abort = True
        time.sleep(1)
        if self.mqtt_client is not None:
            self.mqtt_client.disconnect()
            self.mqtt_client = None
        if self.station is not None:
            self.station.disconnect()
            self.station = None

    # Publish an MQTT message
    def publish(self, topic, value):
        if self.verbose:
            print(f"Publishing value '{value}' in the topic '{topic}'")
        self.mqtt_client.publish(f"{self.mqtt_account_id}/{topic}", value)

    # Subscribe to an MQTT topic
    def subscribe(self, topic):
        if self.verbose:
            print(f"Subscribing '{topic}'")
        if self.callback is None:
            print("ERROR: Subscribe not allowed. The 4th parameter (callback_func) was not provided to the IoTDataHub constructor.")
            sys.exit(1)
        self.mqtt_client.subscribe(f"{self.mqtt_account_id}/{topic}")

    # Connect to Wi-Fi
    def __wifi_connect(self):
        import network
        import time
        station = network.WLAN(network.STA_IF)
        station.active(True)
        station.disconnect()
        station.connect(self.ssid, self.password)
        for t in range(50):
            if station.isconnected():
                break
            time.sleep(0.1)
        return station

    def __get_random_device_id(self, n):
        caracteres = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        resultado = ''
        for _ in range(n):
            resultado += caracteres[random.randint(0, len(caracteres)-1)]
        return resultado

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
try:
    import usocket as socket
except:
    import socket
import ustruct as struct
from ubinascii import hexlify

class MQTTException(Exception):
    pass

class MQTTClient:

    def __init__(self, client_id, server, port=0, user=None, password=None, keepalive=0,
                 ssl=False, ssl_params={}):
        if port == 0:
            port = 8883 if ssl else 1883
        self.client_id = client_id
        self.sock = None
        self.server = server
        self.port = port
        self.ssl = ssl
        self.ssl_params = ssl_params
        self.pid = 0
        self.cb = None
        self.user = user
        self.pswd = password
        self.keepalive = keepalive
        self.lw_topic = None
        self.lw_msg = None
        self.lw_qos = 0
        self.lw_retain = False

    def _send_str(self, s):
        self.sock.write(struct.pack("!H", len(s)))
        self.sock.write(s)

    def _recv_len(self):
        n = 0
        sh = 0
        while 1:
            b = self.sock.read(1)[0]
            n |= (b & 0x7f) << sh
            if not b & 0x80:
                return n
            sh += 7

    def set_callback(self, f):
        self.cb = f

    def set_last_will(self, topic, msg, retain=False, qos=0):
        assert 0 <= qos <= 2
        assert topic
        self.lw_topic = topic
        self.lw_msg = msg
        self.lw_qos = qos
        self.lw_retain = retain

    def connect(self, clean_session=True):
        self.sock = socket.socket()
        addr = socket.getaddrinfo(self.server, self.port)[0][-1]
        self.sock.connect(addr)
        if self.ssl:
            import ussl
            self.sock = ussl.wrap_socket(self.sock, **self.ssl_params)
        premsg = bytearray(b"\x10\0\0\0\0\0")
        msg = bytearray(b"\x04MQTT\x04\x02\0\0")

        sz = 10 + 2 + len(self.client_id)
        msg[6] = clean_session << 1
        if self.user is not None:
            sz += 2 + len(self.user) + 2 + len(self.pswd)
            msg[6] |= 0xC0
        if self.keepalive:
            assert self.keepalive < 65536
            msg[7] |= self.keepalive >> 8
            msg[8] |= self.keepalive & 0x00FF
        if self.lw_topic:
            sz += 2 + len(self.lw_topic) + 2 + len(self.lw_msg)
            msg[6] |= 0x4 | (self.lw_qos & 0x1) << 3 | (self.lw_qos & 0x2) << 3
            msg[6] |= self.lw_retain << 5

        i = 1
        while sz > 0x7f:
            premsg[i] = (sz & 0x7f) | 0x80
            sz >>= 7
            i += 1
        premsg[i] = sz

        self.sock.write(premsg, i + 2)
        self.sock.write(msg)
        #print(hex(len(msg)), hexlify(msg, ":"))
        self._send_str(self.client_id)
        if self.lw_topic:
            self._send_str(self.lw_topic)
            self._send_str(self.lw_msg)
        if self.user is not None:
            self._send_str(self.user)
            self._send_str(self.pswd)
        resp = self.sock.read(4)
        assert resp[0] == 0x20 and resp[1] == 0x02
        if resp[3] != 0:
            raise MQTTException(resp[3])
        return resp[2] & 1

    def disconnect(self):
        self.sock.write(b"\xe0\0")
        self.sock.close()

    def ping(self):
        self.sock.write(b"\xc0\0")

    def publish(self, topic, msg, retain=False, qos=0):
        pkt = bytearray(b"\x30\0\0\0")
        pkt[0] |= qos << 1 | retain
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2
        assert sz < 2097152
        i = 1
        while sz > 0x7f:
            pkt[i] = (sz & 0x7f) | 0x80
            sz >>= 7
            i += 1
        pkt[i] = sz
        #print(hex(len(pkt)), hexlify(pkt, ":"))
        self.sock.write(pkt, i + 1)
        self._send_str(topic)
        if qos > 0:
            self.pid += 1
            pid = self.pid
            struct.pack_into("!H", pkt, 0, pid)
            self.sock.write(pkt, 2)
        self.sock.write(msg)
        if qos == 1:
            while 1:
                op = self.wait_msg()
                if op == 0x40:
                    sz = self.sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self.sock.read(2)
                    rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                    if pid == rcv_pid:
                        return
        elif qos == 2:
            assert 0

    def subscribe(self, topic, qos=0):
        assert self.cb is not None, "Subscribe callback is not set"
        pkt = bytearray(b"\x82\0\0\0")
        self.pid += 1
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, self.pid)
        #print(hex(len(pkt)), hexlify(pkt, ":"))
        self.sock.write(pkt)
        self._send_str(topic)
        self.sock.write(qos.to_bytes(1, "little"))
        while 1:
            op = self.wait_msg()
            if op == 0x90:
                resp = self.sock.read(4)
                #print(resp)
                assert resp[1] == pkt[2] and resp[2] == pkt[3]
                if resp[3] == 0x80:
                    raise MQTTException(resp[3])
                return

    # Wait for a single incoming MQTT message and process it.
    # Subscribed messages are delivered to a callback previously
    # set by .set_callback() method. Other (internal) MQTT
    # messages processed internally.
    def wait_msg(self):
        res = self.sock.read(1)
        self.sock.setblocking(True)
        if res is None:
            return None
        if res == b"":
            raise OSError(-1)
        if res == b"\xd0":  # PINGRESP
            sz = self.sock.read(1)[0]
            assert sz == 0
            return None
        op = res[0]
        if op & 0xf0 != 0x30:
            return op
        sz = self._recv_len()
        topic_len = self.sock.read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self.sock.read(topic_len)
        sz -= topic_len + 2
        if op & 6:
            pid = self.sock.read(2)
            pid = pid[0] << 8 | pid[1]
            sz -= 2
        msg = self.sock.read(sz)
        self.cb(topic, msg)
        if op & 6 == 2:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self.sock.write(pkt)
        elif op & 6 == 4:
            assert 0

    # Checks whether a pending message from server is available.
    # If not, returns immediately with None. Otherwise, does
    # the same processing as wait_msg.
    def check_msg(self):
        self.sock.setblocking(False)
        return self.wait_msg()

    def sleep(self, t):
        while True:
            self.check_msg()
            time.sleep(.1)
            t -= .1
            if t <= 0:
                break
