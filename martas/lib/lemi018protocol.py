"""
LEMI018 ASCII protocol for MARTAS.
"""

import os
import socket
import string
import struct
import sys
from datetime import datetime

from twisted.protocols.basic import LineReceiver
from twisted.python import log

from martas.core import methods as mm


PACK_CODE = "6hLffflllfff"
DATA_KEYS = "[x,y,z,t1,t2,var2,var3,var4,var5]"
DATA_NAMES = "[X,Y,Z,TE,TF,UIN,Latitude,Longitude,Altitude]"
DATA_UNITS = "[nT,nT,nT,deg_C,deg_C,V,deg,deg,m]"
DATA_FACTORS = "[0.001,0.001,0.001,100,100,10,1,1,1]"

# var3 latitude:
# var4 longitude:
# var5 altitude:

class Lemi018Protocol(LineReceiver):
    """Protocol to read newline-delimited LEMI018 ASCII records."""

    delimiter = b"\n"

    def __init__(self, client, sensordict, confdict):
        log.msg("LEMI018 protocol init starting")
        self.client = client
        self.sensordict = sensordict
        self.confdict = confdict

        self.count = 0
        self.sensor = sensordict.get("sensorid")
        self.hostname = socket.gethostname()
        self.printable = set(string.printable)
        self.datalst = []
        self.datacnt = 0
        self.metacnt = 10

        self.last_gps_fix = ""
        self.last_satellites = ""
        self.last_time_diff = ""
        self.last_altitude = ""
        self.last_latitude = ""
        self.last_longitude = ""
        self.last_raw_line = ""

        self.qos = int(confdict.get("mqttqos", 0))
        if self.qos not in [0, 1, 2]:
            self.qos = 0
        log.msg("  -> setting QOS:", self.qos)

        self.pvers = sys.version_info[0]

        debugtest = confdict.get("debug")
        self.debug = False
        if debugtest == "True":
            log.msg("DEBUG - {}: Debug mode activated.".format(self.sensordict.get("protocol")))
            self.debug = True
        else:
            log.msg("  -> Debug mode = {}".format(debugtest))

        log.msg("Initializing LEMI018 finished")

    def connectionMade(self):
        log.msg("  -> {} connected.".format(self.sensor))

    def connectionLost(self, reason):
        log.msg("  -> {} lost.".format(self.sensor))

    def _safe_float(self, value, default=None):
        try:
            if value is None:
                return default
            value = str(value).strip()
            if value == "":
                return default
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value, default=None):
        try:
            if value is None:
                return default
            value = str(value).strip()
            if value == "":
                return default
            return int(float(value))
        except Exception:
            return default

    def _gps_coordinate(self, value, hemisphere):
        value = self._safe_float(value)
        hemisphere = str(hemisphere or "").strip().upper()
        if value is None:
            return float("nan")

        if hemisphere in ["N", "S"]:
            degrees = int(value / 100)
            minutes = value - (degrees * 100)
            decimal = degrees + minutes / 60.0
            if hemisphere == "S":
                decimal *= -1.0
            return decimal

        if hemisphere in ["E", "W"]:
            degrees = int(value / 100)
            minutes = value - (degrees * 100)
            decimal = degrees + minutes / 60.0
            if hemisphere == "W":
                decimal *= -1.0
            return decimal

        return value

    def _buffer_path(self):
        path = os.path.join(self.confdict.get("bufferdirectory"), self.sensor)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def _binary_header(self):
        header = "# MagPyBin {} {} {} {} {} {} {}\n".format(
            self.sensor,
            DATA_KEYS,
            DATA_NAMES,
            DATA_UNITS,
            DATA_FACTORS,
            PACK_CODE,
            struct.calcsize("<" + PACK_CODE),
        )
        if self.pvers > 2:
            return header.encode("ascii")
        return header

    def _meta_header(self):
        return "# MagPyBin {} {} {} {} {} {} {}\n".format(
            self.sensor,
            DATA_KEYS,
            DATA_NAMES,
            DATA_UNITS,
            DATA_FACTORS,
            PACK_CODE,
            struct.calcsize("<" + PACK_CODE),
        )

    def _output_path(self, path, date):
        outpath = os.path.join(path, self.sensor + "_" + date + ".bin")
        header = self._binary_header()

        if not os.path.exists(outpath):
            with open(outpath, "ab") as fh:
                fh.write(header)
                if not header.endswith(b"\n"):
                    fh.write(b"\n")
            return outpath

        with open(outpath, "rb") as fh:
            existing_header = fh.readline()
            rest = fh.read(1)

        if PACK_CODE.encode("ascii") in existing_header:
            return outpath

        if not rest:
            with open(outpath, "wb") as fh:
                fh.write(header)
                if not header.endswith(b"\n"):
                    fh.write(b"\n")
            return outpath

        gpspath = os.path.join(path, self.sensor + "_" + date + "_gps.bin")
        if not os.path.exists(gpspath):
            with open(gpspath, "ab") as fh:
                fh.write(header)
                if not header.endswith(b"\n"):
                    fh.write(b"\n")
        return gpspath

    def _write_binary_record(self, path, date, datearray, bx, by, bz, te, tf, uin, latitude, longitude, altitude):
        outpath = self._output_path(path, date)

        try:
            rec = struct.pack(
                "<" + PACK_CODE,
                datearray[0],
                datearray[1],
                datearray[2],
                datearray[3],
                datearray[4],
                datearray[5],
                datearray[6],
                float(bx),
                float(by),
                float(bz),
                int(round(float(te) * 100.0)),
                int(round(float(tf) * 100.0)),
                int(round(float(uin) * 10.0)),
                float(latitude),
                float(longitude),
                float(altitude),
            )
            with open(outpath, "ab") as fh:
                fh.write(rec)
                fh.write(b"\n")
        except Exception as exc:
            log.err("LEMI018 - Protocol: Could not write data to file: {}".format(exc))

    def _build_dict_payload(self):
        add = (
            "SensorID:{},"
            "StationID:{},"
            "DataPier:{},"
            "SensorModule:{},"
            "SensorGroup:{},"
            "SensorDescription:{},"
            "DataTimeProtocol:{},"
            "DataNTPTimeDelay:{},"
            "DataGPSFixQuality:{},"
            "DataSatellites:{},"
            "DataTimeDiff:{},"
            "DataAltitude:{},"
            "DataLatitude:{},"
            "DataLongitude:{}"
        ).format(
            self.sensordict.get("sensorid", ""),
            self.confdict.get("station", ""),
            self.sensordict.get("pierid", ""),
            self.sensordict.get("protocol", ""),
            self.sensordict.get("sensorgroup", ""),
            self.sensordict.get("sensordesc", "").rstrip(),
            self.sensordict.get("ptime", ""),
            "",
            self.last_gps_fix,
            self.last_satellites,
            self.last_time_diff,
            self.last_altitude,
            self.last_latitude,
            self.last_longitude,
        )
        return add

    def _normalize_parts(self, parts):
        parts = [p.strip() for p in parts]

        if len(parts) < 12:
            return None

        if len(parts) > 20:
            parts = parts[:20]

        if 12 < len(parts) < 20:
            parts = parts + [""] * (20 - len(parts))

        return parts

    def processLemi018Line(self, line):
        self.last_raw_line = line

        if isinstance(line, bytes):
            try:
                line = line.decode("ascii", errors="ignore")
            except Exception:
                line = str(line)

        line = line.strip().replace("\r", "")
        if not line:
            return "", self._meta_header()

        if line.startswith("#"):
            return "", self._meta_header()

        parts = self._normalize_parts(line.strip().split())
        if parts is None:
            if self.debug:
                log.msg("LEMI018 - Protocol: too few fields ({}): {}".format(len(line.strip().split(",")), line))
            return "", self._meta_header()

        try:
            year = self._safe_int(parts[0])
            month = self._safe_int(parts[1])
            day = self._safe_int(parts[2])
            hour = self._safe_int(parts[3])
            minute = self._safe_int(parts[4])
            second = self._safe_int(parts[5])

            bx = self._safe_float(parts[6])
            by = self._safe_float(parts[7])
            bz = self._safe_float(parts[8])
            te = self._safe_float(parts[9])
            tf = self._safe_float(parts[10])
            uin = self._safe_float(parts[11])

            required = [year, month, day, hour, minute, second, bx, by, bz, te, tf, uin]
            if any(v is None for v in required):
                if self.debug:
                    log.msg("LEMI018 - Protocol: invalid core fields in line: {}".format(line))
                return "", self._meta_header()

            gpstime = datetime(year, month, day, hour, minute, second)
            gps_time = datetime.strftime(gpstime, "%Y-%m-%d %H:%M:%S.%f")
            date = datetime.strftime(gpstime, "%Y-%m-%d")
            datearray = mm.time_to_array(gps_time)
            altitude = float("nan")
            latitude = float("nan")
            longitude = float("nan")

            if len(parts) >= 20:
                altitude = self._safe_float(parts[12], float("nan"))
                lat_hemi = parts[14]
                lon_hemi = parts[16]
                satellites = parts[17]
                gps_fix = parts[18]
                time_diff = parts[19]
                latitude = self._gps_coordinate(parts[13], lat_hemi)
                longitude = self._gps_coordinate(parts[15], lon_hemi)

                self.last_altitude = "" if altitude != altitude else str(altitude)
                self.last_latitude = "" if latitude != latitude else str(latitude)
                self.last_longitude = "" if longitude != longitude else str(longitude)
                self.last_satellites = satellites
                self.last_gps_fix = gps_fix
                self.last_time_diff = time_diff
            else:
                self.last_altitude = ""
                self.last_latitude = ""
                self.last_longitude = ""
                self.last_satellites = ""
                self.last_gps_fix = ""
                self.last_time_diff = ""

            self._write_binary_record(
                self._buffer_path(),
                date,
                datearray,
                bx,
                by,
                bz,
                te,
                tf,
                uin,
                latitude,
                longitude,
                altitude,
            )

            datalst = mm.time_to_array(gps_time)
            datalst.append(float(bx))
            datalst.append(float(by))
            datalst.append(float(bz))
            datalst.append(int(round(float(te) * 100.0)))
            datalst.append(int(round(float(tf) * 100.0)))
            datalst.append(int(round(float(uin) * 10.0)))
            datalst.append(float(latitude))
            datalst.append(float(longitude))
            datalst.append(float(altitude))

            dataarray = ",".join(map(str, datalst))
            return dataarray, self._meta_header()

        except Exception as exc:
            log.err("LEMI018 - Protocol: parse error: {} | line={!r}".format(exc, line))
            return "", self._meta_header()

    def lineReceived(self, line):
        topic = self.confdict.get("station") + "/" + self.sensordict.get("sensorid")

        try:
            dataarray, head = self.processLemi018Line(line)
        except Exception as exc:
            log.err("LEMI018 - Protocol: Error while parsing data: {}".format(exc))
            return

        if not dataarray:
            return

        senddata = False
        coll = int(self.sensordict.get("stack"))

        if coll > 1:
            self.metacnt = 1
            if self.datacnt < coll:
                self.datalst.append(dataarray)
                self.datacnt += 1
            else:
                senddata = True
                dataarray = ";".join(self.datalst)
                self.datalst = []
                self.datacnt = 0
        else:
            senddata = True

        if senddata:
            self.client.publish(topic + "/data", dataarray, qos=self.qos)
            if self.count == 0:
                self.client.publish(topic + "/dict", self._build_dict_payload(), qos=self.qos)
                self.client.publish(topic + "/meta", head, qos=self.qos)
            self.count += 1
            if self.count >= self.metacnt:
                self.count = 0
