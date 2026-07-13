"""
LEMI417 binary protocol for MARTAS.
"""

import datetime
import os
import struct

from twisted.protocols.basic import LineReceiver
from twisted.python import log

from martas.core import methods as mm

FRAME_SIGNATURE = b"L417"
FRAME_SIZE = 47
PACK_CODE = "6hLfffhhllll"


def data2_to_double(z):
    tmp = z[1] * (2**8) + z[0]
    if z[1] > 128:
        tmp -= 2**16
    return tmp / 100.0


def data3_to_double(z):
    tmp = z[2] * (2**16) + z[1] * (2**8) + z[0]
    if z[2] > 128:
        tmp -= 2**24
    return tmp / 100.0


def data4_to_double(z):
    tmp = z[3] * (2**24) + z[2] * (2**16) + z[1] * (2**8) + z[0]
    if z[3] > 128:
        tmp -= 2**32
    return tmp / 100.0


def bcd_to_int(value):
    return int(format(value, "02x"))


class Lemi417Protocol(LineReceiver):
    def __init__(self, client, sensordict, confdict):
        log.msg("LEMI417 protocol init starting")
        print("LEMI417 protocol using patched MagPyBin writer: {}".format(__file__))

        self.client = client
        self.sensordict = sensordict
        self.confdict = confdict

        self.sensor = sensordict.get("sensorid")
        self.station = confdict.get("station")
        self.qos = int(confdict.get("mqttqos", 0))
        self.buffer = b""
        self.pvers = 2
        self.datacnt = 0
        self.debug_chunks = 0
        self.no_signature_logs = 0
        self.partial_frame_logs = 0

        self._init_file()

        log.msg("Initializing LEMI417 finished")

    def _init_file(self):
        bufferpath = self.confdict.get("bufferpath") or self.confdict.get("mqttbuffer") or self.confdict.get("bufferdirectory")
        if not bufferpath:
            raise Exception("No MARTAS buffer path defined")
        self.sensorpath = os.path.join(bufferpath, self.sensor)

        os.makedirs(self.sensorpath, exist_ok=True)

        self.filename = os.path.join(
            self.sensorpath,
            self.sensor + "_" + datetime.datetime.utcnow().strftime("%Y-%m-%d") + ".bin",
        )

        header = self._binary_header()
        write_header = not os.path.exists(self.filename)
        if not write_header:
            with open(self.filename, "rb") as fh:
                existing_header = fh.readline()
                rest = fh.read(1)
            if PACK_CODE.encode("ascii") not in existing_header and not rest:
                log.msg("LEMI417 replacing header-only file with updated pack code {}".format(PACK_CODE))
                write_header = True

        if write_header:
            with open(self.filename, "wb") as fh:
                fh.write(header)

    def _binary_header(self):
        return "# MagPyBin {} {} {} {} {} {} {}\n".format(
            self.sensor,
            "[x,y,z,t1,t2,var1,var2,var3,var4]",
            "[X,Y,Z,Te,Tf,E1,E2,E3,E4]",
            "[nT,nT,nT,deg,deg,V,V,V,V]",
            "[0.001,0.001,0.001,100,100,100,100,100,100]",
            PACK_CODE,
            struct.calcsize("<" + PACK_CODE),
        ).encode("ascii")

    def dataReceived(self, data):
        if self.debug_chunks < 20:
            preview = data[:64].hex()
            log.msg("LEMI417 dataReceived chunk {}: len={} hex={}".format(self.debug_chunks + 1, len(data), preview))
            self.debug_chunks += 1

        self.buffer += data

        while True:
            idx = self.buffer.find(FRAME_SIGNATURE)
            if idx < 0:
                if self.no_signature_logs < 20 and self.buffer:
                    log.msg("LEMI417 frame signature {!r} not found in buffer len={} hex={}".format(
                        FRAME_SIGNATURE,
                        len(self.buffer),
                        self.buffer[:64].hex(),
                    ))
                    self.no_signature_logs += 1
                self.buffer = self._signature_prefix_suffix(self.buffer)
                return

            if len(self.buffer) < idx + FRAME_SIZE:
                if self.partial_frame_logs < 20:
                    log.msg("LEMI417 partial frame: idx={} buffer_len={} required={}".format(idx, len(self.buffer), idx + FRAME_SIZE))
                    self.partial_frame_logs += 1
                return

            frame = self.buffer[idx:idx + FRAME_SIZE]
            self.buffer = self.buffer[idx + FRAME_SIZE:]
            payload = frame[4:]

            try:
                unpacked = struct.unpack("2B B B B B B B B 2B 3B 3B 3B 4B 4B 4B 4B 2B 2B 2B B", payload)
            except Exception as exc:
                log.msg("LEMI417 unpack error {}".format(exc))
                continue

            year = bcd_to_int(unpacked[3])
            month = bcd_to_int(unpacked[4])
            day = bcd_to_int(unpacked[5])
            hour = bcd_to_int(unpacked[6])
            minute = bcd_to_int(unpacked[7])
            second = bcd_to_int(unpacked[8])

            try:
                gpstime = datetime.datetime(2000 + year, month, day, hour, minute, second)
            except Exception as exc:
                log.msg("LEMI417 invalid timestamp y={} m={} d={} h={} min={} s={}: {}".format(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    exc,
                ))
                continue

            bx = data3_to_double(unpacked[11:14])
            by = data3_to_double(unpacked[14:17])
            bz = data3_to_double(unpacked[17:20])

            e1 = data4_to_double(unpacked[20:24])
            e2 = data4_to_double(unpacked[24:28])
            e3 = data4_to_double(unpacked[28:32])
            e4 = data4_to_double(unpacked[32:36])

            tf = data2_to_double(unpacked[36:38])
            te = data2_to_double(unpacked[38:40])

            datalst = mm.time_to_array(gpstime.strftime("%Y-%m-%d %H:%M:%S.%f"))
            datalst += [
                bx,
                by,
                bz,
                int(te * 100),
                int(tf * 100),
                int(e1 * 100),
                int(e2 * 100),
                int(e3 * 100),
                int(e4 * 100),
            ]

            dataarray = ",".join(map(str, datalst))
            topic = self.station + "/" + self.sensor

            try:
                self.client.publish(topic + "/data", dataarray, qos=self.qos)
            except Exception as exc:
                log.msg("MQTT publish failed {}".format(exc))

            try:
                rec = struct.pack("<" + PACK_CODE, *mm.array_to_bin(datalst))
                with open(self.filename, "ab") as fh:
                    fh.write(rec)
                    fh.write(b"\n")
            except Exception as exc:
                log.msg("LEMI417 bin write error {}".format(exc))

            self.datacnt += 1

    def _signature_prefix_suffix(self, data):
        """Keep bytes that could be the start of a split frame signature."""
        max_len = min(len(data), len(FRAME_SIGNATURE) - 1)
        for length in range(max_len, 0, -1):
            suffix = data[-length:]
            if FRAME_SIGNATURE.startswith(suffix):
                return suffix
        return b""
