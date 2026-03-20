#!/usr/bin/env python3
"""
传感器数据采集 + 云平台上报程序

功能：
1. 通过串口读取传感器数据（压力+测距）
2. 上报数据到新大陆云平台(NLECloud)
3. 保持TCP长连接，定时心跳维持在线状态
4. 支持云平台命令下发控制执行器
"""

import socket
import threading
import time
import logging
import requests
import json
import sys
import select
import serial

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class Config:
    SERIAL_PORT = 'COM3'
    BAUDRATE = 2400
    SERIAL_TIMEOUT = 3

    MODULE_ID_485 = "0034222001"
    MODULE_ID_ZX = '18220149'

    NLE_HOST = 'ndp.nlecloud.com'
    NLE_PORT = 8600
    DEVICE_TAG = '646585223'
    DEVICE_KEY = '68e8825668424b1c9123ca141bb02862'
    DEVICE_ID = '1421801'

    USERNAME = '13175440866'
    PASSWORD = '070725'

    UPLOAD_INTERVAL = 15
    CMD_POLL_INTERVAL = 3


def auto_detect_serial_port():
    import serial.tools.list_ports
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        logger.error("[串口] 未检测到任何串口设备")
        return None
    logger.info(f"[串口] 检测到 {len(ports)} 个串口: {[p.device for p in ports]}")
    for port_info in ports:
        port = port_info.device
        logger.info(f"[串口] 尝试串口: {port}")
        try:
            ser = serial.Serial(port, 2400, timeout=3)
            ser.close()
            logger.info(f"[串口] 找到可用串口: {port}")
            return port
        except Exception as e:
            logger.warning(f"[串口] {port} 不可用: {e}")
            continue
    logger.error("[串口] 未找到可用的串口设备")
    return None


class UnifiedSensorReader:
    def __init__(self, port, baudrate, timeout, module_id_485, module_id_zx):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.module_id_485 = module_id_485
        self.module_id_zx = module_id_zx
        self.ser = None

    def _open_serial(self):
        if self.ser is None or not self.ser.is_open:
            self.ser = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
            time.sleep(0.1)

    def _close_serial(self):
        if self.ser and self.ser.is_open:
            self.ser.close()
            self.ser = None

    def calculate_checksum(self, data):
        ascii_sum = sum(ord(c) for c in data)
        checksum_val = 128 + (ascii_sum % 128)
        return checksum_val

    def parse_485_response(self, response):
        if '&' not in response or 'A;' not in response:
            return None
        result = {}
        try:
            response_clean = response.strip().rstrip('!')
            before_star, after_star = response_clean.split('*', 1)
            header_part = before_star.split(';', 1)[1] if ';' in before_star else ""
            header_fields = header_part.split(',')
            result['module_addr'] = header_fields[5] if len(header_fields) > 5 else 'N/A'
            result['module_voltage'] = header_fields[7] if len(header_fields) > 7 else 'N/A'
            after_star_clean = after_star.rstrip(',!*')
            data_fields = after_star_clean.split(',')
            channels = []
            idx = 0
            while idx < len(data_fields) - 1:
                field = data_fields[idx]
                next_field = data_fields[idx+1] if idx+1 < len(data_fields) else ''
                if (field + next_field) in ['+1', '+2', '+3', '+4', '-1', '-2', '-3', '-4']:
                    channel = field + next_field
                    sensor_id = data_fields[idx+2] if idx+2 < len(data_fields) else 'N/A'
                    data2 = data_fields[idx+7] if idx+7 < len(data_fields) else 'N/A'
                    channel_data = {
                        'channel': channel,
                        'sensor_id': sensor_id,
                        'pressure': float(data2) if data2 != 'N/A' else None
                    }
                    channels.append(channel_data)
                idx += 1
            result['channels'] = channels
        except Exception as e:
            logger.error(f"[485解析] 解析失败: {e}")
            return None
        return result

    def parse_zx_response(self, response):
        if not response or len(response) < 2:
            return None
        raw = response[1:-2]
        measurements = []
        type_divisors = {
            70: {'data1_divisor': 981, 'data2_divisor': 981},
            71: {'data1_divisor': 981, 'data2_divisor': 981},
            72: {'data1_divisor': 981, 'data2_divisor': 981},
            75: {'data1_divisor': 981, 'data2_divisor': 981},
            76: {'data1_divisor': 981, 'data2_divisor': 981}
        }

        def is_digit(b):
            return 48 <= b <= 57

        def skip_checksum(raw, i):
            if i < len(raw) and raw[i] >= 128:
                i += 1
            return i

        i = 0
        while i < len(raw):
            if i >= len(raw):
                break
            measurement = {}
            if raw[i] == 84:
                i += 1
                time_str = ''
                while i < len(raw) and is_digit(raw[i]):
                    time_str += chr(raw[i])
                    i += 1
                measurement['time'] = time_str
                i = skip_checksum(raw, i)
            if i + 2 <= len(raw):
                type_code_str = ''
                for j in range(2):
                    if is_digit(raw[i+j]):
                        type_code_str += chr(raw[i+j])
                type_code = int(type_code_str) if type_code_str else 0
                measurement['type_code'] = type_code
                i += 2
                i = skip_checksum(raw, i)
            if i + 8 <= len(raw):
                sensor_id = ''
                for j in range(8):
                    if is_digit(raw[i+j]):
                        sensor_id += chr(raw[i+j])
                measurement['sensor_id'] = sensor_id
                i += 8
                i = skip_checksum(raw, i)
            data1_str = ''
            while i < len(raw) and is_digit(raw[i]):
                data1_str += chr(raw[i])
                i += 1
            data1_raw = int(data1_str) if data1_str else 0
            measurement['data1_raw'] = data1_raw
            divisors = type_divisors.get(type_code, {'data1_divisor': 1, 'data2_divisor': 1})
            measurement['data1'] = data1_raw / divisors['data1_divisor']
            i = skip_checksum(raw, i)
            data2_str = ''
            while i < len(raw) and is_digit(raw[i]):
                data2_str += chr(raw[i])
                i += 1
            data2_raw = int(data2_str) if data2_str else 0
            measurement['data2_raw'] = data2_raw
            measurement['data2'] = data2_raw / divisors['data2_divisor']
            i = skip_checksum(raw, i)
            temp_str = ''
            while i < len(raw) and is_digit(raw[i]):
                temp_str += chr(raw[i])
                i += 1
            temp_raw = int(temp_str) if temp_str else 0
            measurement['temp_raw'] = temp_raw
            measurement['temp'] = temp_raw / 10.0
            if measurement:
                measurements.append(measurement)
            i += 1
        return measurements

    def read_both(self):
        pressure = None
        distance = None

        self._open_serial()

        for cs in [0, 10, 20, 130]:
            cmd = f"@{self.module_id_485}A*{cs}!"
            self.ser.flushInput()
            self.ser.write(cmd.encode('ascii'))
            time.sleep(0.5)
            response = self.ser.read(200)
            resp_str = response.decode('ascii', errors='ignore')
            result = self.parse_485_response(resp_str)
            if result and result['channels']:
                pressures = []
                for ch in result['channels']:
                    if ch['pressure'] is not None:
                        pressures.append(ch['pressure'])
                if pressures:
                    pressure = sum(pressures) / len(pressures)
                    break

        time.sleep(0.5)

        data = self.module_id_zx + 'A'
        checksum = self.calculate_checksum(data)
        command = f'#{data}'.encode('latin1') + bytes([checksum]) + b'!'
        self.ser.flushInput()
        self.ser.write(command)
        time.sleep(6)
        response = self.ser.read(2000)
        if response:
            measurements = self.parse_zx_response(response)
            if measurements:
                for m in measurements:
                    if m.get('type_code') == 72:
                        distance = m.get('data1')
                        break

        self._close_serial()
        return pressure, distance


class NLECloudClient:
    def __init__(self, host, port, device_tag, device_key):
        self.host = host
        self.port = port
        self.device_tag = device_tag
        self.device_key = device_key
        self.sock = None
        self.connected = False
        self.running = True
        self.lock = threading.Lock()
        self.callback = None
        self.last_heartbeat = 0
        self.heartbeat_interval = 30

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
            self.connected = False

    def connect(self):
        with self.lock:
            try:
                self._close_socket()
                time.sleep(0.5)

                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(10)
                self.sock.connect((self.host, self.port))

                handshake = json.dumps({
                    "t": 1,
                    "device": self.device_tag,
                    "key": self.device_key,
                    "ver": "v1.0"
                })

                self.sock.sendall(handshake.encode() + b'\r\n')
                response = self.sock.recv(1024)

                resp_json = json.loads(response.decode().strip())
                if resp_json.get('status') == 0:
                    self.connected = True
                    self.last_heartbeat = time.time()
                    logger.info("[云平台] 连接成功")
                    return True
                else:
                    logger.error(f"[云平台] 连接失败: {response}")
            except Exception as e:
                logger.error(f"[云平台] 连接异常: {e}")

        self.connected = False
        return False

    def send_heartbeat(self):
        current_time = time.time()
        if current_time - self.last_heartbeat >= self.heartbeat_interval:
            if self.send_data({"t": 5}):
                self.last_heartbeat = current_time

    def send_data(self, data):
        if not self.connected:
            if not self.connect():
                return False

        try:
            with self.lock:
                self.sock.settimeout(3)
                self.sock.sendall(json.dumps(data).encode() + b'\r\n')
                try:
                    response = self.sock.recv(1024)
                except socket.timeout:
                    pass
                return True
        except socket.timeout:
            logger.warning("[云平台] 发送超时")
            self._close_socket()
            return False
        except Exception as e:
            logger.warning(f"[云平台] 发送异常: {e}")
            self._close_socket()
            return False

    def upload_data(self, pressure, distance):
        for retry in range(3):
            try:
                datas = {
                    "m_Soil": round(pressure, 2) if pressure is not None else 0,
                    "m_Laser": round(distance, 2) if distance is not None else 0
                }

                data = {
                    "t": 3,
                    "datatype": 1,
                    "datas": datas
                }

                if self.send_data(data):
                    logger.info(f"[云平台] 数据上报成功: 压力={datas['m_Soil']} MPa, 测距={datas['m_Laser']} m")
                    return True

            except Exception as e:
                logger.warning(f"[云平台] 上报异常 (重试{retry+1}): {e}")
                self.connected = False
                time.sleep(1)

        logger.error(f"[云平台] 上报失败")
        return False

    def set_callback(self, callback):
        self.callback = callback

    def receive_loop(self):
        while self.running:
            try:
                if not self.connected:
                    if not self.connect():
                        time.sleep(2)
                        continue

                with self.lock:
                    self.sock.settimeout(2)
                    try:
                        response = self.sock.recv(4096)
                        if response:
                            msgs = response.decode().strip().split('\r\n')
                            for msg_str in msgs:
                                if not msg_str:
                                    continue
                                try:
                                    msg = json.loads(msg_str)
                                    msg_type = msg.get('t')
                                    if msg_type == 5:
                                        apitag = msg.get('apitag', '')
                                        cmd_data = msg.get('data')
                                        logger.info(f"[云平台] 收到命令: {apitag} = {cmd_data}")
                                        if self.callback:
                                            self.callback(apitag, cmd_data)
                                except json.JSONDecodeError:
                                    pass
                    except socket.timeout:
                        pass

                self.send_heartbeat()

            except Exception as e:
                logger.warning(f"[云平台] 接收循环异常: {e}")
                self._close_socket()
                time.sleep(1)

    def stop(self):
        self.running = False
        self._close_socket()


class NLECloudAPI:
    def __init__(self, device_id, username, password):
        self.device_id = device_id
        self.username = username
        self.password = password
        self.token = None

    def login(self):
        try:
            resp = requests.post(
                'http://api.nlecloud.com/users/login',
                json={'Account': self.username, 'Password': self.password},
                timeout=10
            )
            result = resp.json()
            if result.get('Status') == 0:
                self.token = result['ResultObj']['AccessToken']
                logger.info("[HTTP] 登录成功")
                return True
        except Exception as e:
            logger.error(f"[HTTP] 登录失败: {e}")
        return False

    def poll_command(self, last_cmds, callback):
        try:
            if not self.token:
                if not self.login():
                    return last_cmds

            url = f'http://api.nlecloud.com/devices/{self.device_id}'
            headers = {'AccessToken': self.token}
            resp = requests.get(url, headers=headers, timeout=10)
            result = resp.json()

            if result.get('Status') == 0:
                sensors = result.get('ResultObj', {}).get('Sensors', [])
                for sensor in sensors:
                    apitag = sensor.get('ApiTag', '')
                    if apitag in ['do_light', 'temp']:
                        cmd_value = sensor.get('Value', '')

                        if apitag not in last_cmds:
                            last_cmds[apitag] = None

                        if cmd_value and str(cmd_value) != str(last_cmds[apitag]):
                            logger.info(f"[HTTP] 收到命令: {apitag} = {cmd_value}")
                            last_cmds[apitag] = cmd_value

                            try:
                                if isinstance(cmd_value, bool):
                                    if callback:
                                        callback(apitag, cmd_value)
                                else:
                                    cmd_data = json.loads(cmd_value)
                                    cmd_str = cmd_data.get('CmdStr', '').lower()
                                    if callback:
                                        callback(apitag, cmd_str == 'true')
                            except Exception as e:
                                logger.error(f"[HTTP] 命令解析错误: {e}")

        except Exception as e:
            logger.error(f"[HTTP] 命令轮询异常: {e}")
            if 'token' in str(e).lower() or 'unauthorized' in str(e).lower():
                self.token = None

        return last_cmds


def main():
    cfg = Config()

    detected_port = auto_detect_serial_port()
    if detected_port is None:
        logger.error("[主程序] 未找到可用串口，程序退出")
        return

    logger.info(f"[主程序] 使用串口: {detected_port}")

    sensor = UnifiedSensorReader(
        port=detected_port,
        baudrate=cfg.BAUDRATE,
        timeout=cfg.SERIAL_TIMEOUT,
        module_id_485=cfg.MODULE_ID_485,
        module_id_zx=cfg.MODULE_ID_ZX
    )

    nle_tcp = NLECloudClient(cfg.NLE_HOST, cfg.NLE_PORT, cfg.DEVICE_TAG, cfg.DEVICE_KEY)
    nle_http = NLECloudAPI(cfg.DEVICE_ID, cfg.USERNAME, cfg.PASSWORD)

    logger.info("=" * 50)
    logger.info("传感器数据采集 + 云平台上报程序")
    logger.info("采集数据: 压力(485) + 测距(ZX)")
    logger.info(f"串口: {detected_port} @ {cfg.BAUDRATE} bps")
    logger.info(f"云平台: {cfg.NLE_HOST}:{cfg.NLE_PORT}")
    logger.info(f"设备标识: {cfg.DEVICE_TAG}")
    logger.info("=" * 50)

    nle_http.login()
    nle_tcp.connect()

    cmd_http_t = threading.Thread(target=lambda: loop_http_poll(nle_http), daemon=True)
    cmd_http_t.start()
    logger.info("[线程] HTTP命令轮询已启动")

    count = 0
    while True:
        try:
            count += 1
            logger.info(f"=== 第{count}次采集 ===")

            pressure, distance = sensor.read_both()

            if pressure is not None:
                logger.info(f"[485-压力] 数据: {pressure:.2f} MPa")
            else:
                logger.warning("[485-压力] 读取失败")

            if distance is not None:
                logger.info(f"[ZX-测距] 数据: {distance:.2f} m")
            else:
                logger.warning("[ZX-测距] 读取失败")

            nle_tcp.upload_data(pressure, distance)

            time.sleep(cfg.UPLOAD_INTERVAL)

        except Exception as e:
            logger.error(f"[主循环] 异常: {e}")
            time.sleep(5)


def loop_http_poll(nle_api):
    last_cmds = {}
    while True:
        last_cmds = nle_api.poll_command(last_cmds, None)
        time.sleep(3)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("程序已停止")
        sys.exit(0)