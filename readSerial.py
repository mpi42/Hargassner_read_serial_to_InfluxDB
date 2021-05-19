#!/usr/bin/env python

import datetime
import time
import serial
import io
import logging
from influxdb import InfluxDBClient
import json
import pytz

# start the logger
logging.basicConfig(filename='/mnt/usbdata/LogFiles/readSerial.log',
                    filemode='a',
                    format='%(asctime)s | %(name)s | %(levelname)s || %(message)s',
                    ddatefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)

logging.info("HeizungsPi - Lesen von Serieller und Schreiben in InfluxDB Skript gestartet")

# logging.debug('This is a debug message')
# logging.info('This is an info message')
# logging.warning('This is a warning message')
# logging.error('This is an error message')
# logging.critical('This is a critical message')

# influx configuration -
ifuser = "heizung"
ifpass = "heizung"
ifdb = "heizung"
ifhost = "127.0.0.1"
ifport = 8086

# connect to influx
ifclient = InfluxDBClient(ifhost, ifport, ifuser, ifpass, ifdb)
logging.info("Verbindung zu InfluxDB hergestellt")

# config serial connection
ser = serial.Serial(
    port='/dev/ttyUSB0',
    baudrate=19200,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=1
)

sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser), encoding='iso-8859-1')

logging.info("Serielle Verbindung hergestellt")

curentTimeOfHeatingStation = ''
timeInUTC = ''

#Heizungsdaten
heatingData = {} # fuer die live daten
heatingData['serverTime'] = ""
heatingData['anlagenTime'] = ""
heatingData['data'] = {}
heatingData['data']['Primaerluft'] = ""
heatingData['data']['P'] = ""
heatingData['data']['O2'] = ""
heatingData['data']['Kesseltemp'] = ""
heatingData['data']['Rauchtemp'] = ""
heatingData['data']['AT'] = ""
heatingData['data']['ATgem'] = ""
heatingData['data']['HK1VL'] = ""
heatingData['data']['HK1Soll'] = ""
heatingData['data']['HK2Soll'] = ""
heatingData['data']['KesselRL'] = ""
heatingData['data']['Foerderer'] = ""
heatingData['data']['KesselSoll'] = ""
heatingData['data']['HK3VL'] = ""
heatingData['data']['HK3Soll'] = ""
heatingData['data']['Boiler3'] = ""
heatingData['data']['HK5VL'] = ""
heatingData['data']['HK5Soll'] = ""
heatingData['data']['Boiler5'] = ""

# nur zum testen
heatingData['devdata'] = ""

nowSeconds = time.time()
historySeconds = time.time()


tz = pytz.timezone('Europe/Vienna')

# Read the serial infinite
while True:
    nowSeconds = time.time()
    heatingData['serverTime'] = datetime.datetime.now().isoformat()  # aktuelle serverzeit als unix timestamp
    data = sio.readline().strip()
    if data:
        if data.startswith('tm '):  # its the time of the heating station
            tempString = data.split()
            curentTimeOfHeatingStation = datetime.datetime.strptime(tempString[1] + " " + tempString[2],
                                                                    '%Y-%m-%d %H:%M:%S')  # string zerlegen und ein datumsobjekt daraus bauen
            aware_datetime = tz.localize(curentTimeOfHeatingStation) 
            timeInUTC = aware_datetime.astimezone(pytz.utc)       
            # print(curentTimeOfHeatingStation)
            # print(aware_datetime.astimezone(pytz.utc))
            # print("start with tm")
            # print(curtime)

            # write to actual data json
            heatingData['anlagenTime'] = curentTimeOfHeatingStation.isoformat()  # as unix timestamp livedata
            logging.debug("tm data: %s", data)
        elif data.startswith('z '):  # meldungen
            # print("start with z")
            # print(data)
            logging.debug("z data: %s", data)
            #write the meldung to influxdb
            tempMeldung = data.replace('z ', '')
            #format the data as a single measurement for influx
            bodyMeldung = [
                {
                    "measurement": "meldungen",
                    "time": timeInUTC,
                    "fields": {
                        "meldung": tempMeldung
                    }
                }
             ]
            # write the measurement
            # write everytime
            ifclient.write_points(bodyMeldung)
        elif data.startswith('pm '):  # daten von der anlage
            tempArray = data.split()  # bei jedem leerzeichen trennen
            # Daten zum testen ausgeben und schreiben
            heatingData['devdata'] = data
            # print("start with pm")
            # print(data)
            logging.debug("pm data: %s", data)
            #heizungsdaten eintragen
            heatingData['data']['Primaerluft'] = tempArray[1]
            heatingData['data']['P'] = tempArray[2]
            heatingData['data']['O2'] = tempArray[3]
            heatingData['data']['Kesseltemp'] = tempArray[4]
            heatingData['data']['Rauchtemp'] = tempArray[5]
            heatingData['data']['AT'] = tempArray[6]
            heatingData['data']['ATgem'] = tempArray[7]
            heatingData['data']['HK1VL'] = tempArray[8]
            heatingData['data']['HK1Soll'] = tempArray[10]
            heatingData['data']['HK2Soll'] = tempArray[11]
            heatingData['data']['KesselRL'] = tempArray[12]
            heatingData['data']['Foerderer'] = tempArray[14]
            heatingData['data']['KesselSoll'] = tempArray[15]
            heatingData['data']['HK3VL'] = tempArray[20]
            heatingData['data']['HK3Soll'] = tempArray[22]
            heatingData['data']['Boiler3'] = tempArray[25]
            heatingData['data']['HK5VL'] = tempArray[27]
            heatingData['data']['HK5Soll'] = tempArray[29]
            heatingData['data']['Boiler5'] = tempArray[31]

            # write the measurement only every minute to reduce the data
            if (nowSeconds - historySeconds) > 60:
                # format the data as a single measurement for influx
                bodyData = [
                    {
                        "measurement": "heizungsdaten",
                        "time": timeInUTC,
                        "fields": {
                            "Primaerluft": float(heatingData['data']['Primaerluft']),
                            "P": float(heatingData['data']['P']),
                            "O2": float(heatingData['data']['O2']),
                            "Kesseltemp": float(heatingData['data']['Kesseltemp']),
                            "Rauchtemp": float(heatingData['data']['Rauchtemp']),
                            "AT": float(heatingData['data']['AT']),
                            "ATgem": float(heatingData['data']['ATgem']),
                            "HK1VL": float(heatingData['data']['HK1VL']),
                            "HK1Soll": float(heatingData['data']['HK1Soll']),
                            "HK2Soll": float(heatingData['data']['HK2Soll']),
                            "KesselRL": float(heatingData['data']['KesselRL']),
                            "Foerderer": float(heatingData['data']['Foerderer']),
                            "KesselSoll": float(heatingData['data']['KesselSoll']),
                            "HK3VL": float(heatingData['data']['HK3VL']),
                            "HK3Soll": float(heatingData['data']['HK3Soll']),
                            "Boiler3": float(heatingData['data']['Boiler3']),
                            "HK5VL": float(heatingData['data']['HK5VL']),
                            "HK5Soll": float(heatingData['data']['HK5Soll']),
                            "Boiler5": float(heatingData['data']['Boiler5'])
                        }
                    }
                ]
                ifclient.write_points(bodyData)
                historySeconds = time.time()
                #print("history schreiben")
        else:  # shitty data
            # print("start with something else !!!!")
            # print(data)
            logging.info("xxx data: %s", data)
	#when if finished write out the live data json for the live visu
    with open('/mnt/usbdata/www/html/data/dataLive.json', 'w') as outfile:
        json.dump(heatingData, outfile)


logging.error("HeizungsPi - Lesen von Serieller und Schreiben in InfluxDB Skript BEENDET")
