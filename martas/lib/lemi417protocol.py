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


class Lemi417Protocol(LineReceiver):
    def __init__(self, client, sensordict, confdict):
        log.msg("LEMI417 protocol init starting")

        self.client = client
        self.sensordict = sensordict
        self.confdict = confdict

        self.sensor = sensordict.get("sensorid")
        self.station = confdict.get("station")
        self.qos = int(confdict.get("mqttqos", 0))
        self.buffer = b""
        self.pvers = 2
        self.datacnt = 0

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

        if not os.path.exists(self.filename):
            header = "# MagPyBin {} {} {} {} {} {} {}\n".format(
                self.sensor,
                "[x,y,z,t1,t2,var1,var2,var3,var4]",
                "[X,Y,Z,Te,Tf,E1,E2,E3,E4]",
                "[nT,nT,nT,deg,deg,V,V,V,V]",
                "[0.001,0.001,0.001,100,100,100,100,100,100]",
                "6hLfffhhhhhh",
                struct.calcsize("<6hLfffhhhhhh"),
            )
            with open(self.filename, "wb") as fh:
                fh.write(header.encode("ascii"))

    def dataReceived(self, data):
        self.buffer += data

        while True:
            idx = self.buffer.find(FRAME_SIGNATURE)
            if idx < 0:
                self.buffer = b""
                return

            if len(self.buffer) < idx + FRAME_SIZE:
                return

            frame = self.buffer[idx:idx + FRAME_SIZE]
            self.buffer = self.buffer[idx + FRAME_SIZE:]
            payload = frame[4:]

            try:
                unpacked = struct.unpack("2B B B B B B B B 2B 3B 3B 3B 4B 4B 4B 4B 2B 2B 2B B", payload)
            except Exception as exc:
                log.msg("LEMI417 unpack error {}".format(exc))
                continue

            year = unpacked[3]
            month = unpacked[4]
            day = unpacked[5]
            hour = unpacked[6]
            minute = unpacked[7]
            second = unpacked[8]

            try:
                gpstime = datetime.datetime(2000 + year, month, day, hour, minute, second)
            except Exception:
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
                rec = struct.pack("<6hLfffhhhhhh", *mm.array_to_bin(datalst))
                with open(self.filename, "ab") as fh:
                    fh.write(rec)
            except Exception as exc:
                log.msg("LEMI417 bin write error {}".format(exc))

            self.datacnt += 1
