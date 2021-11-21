import socket
import threading
import redis

from time import gmtime, strftime
import pickle

import configparser
from optparse import OptionParser

from gps import GPSTerminal

class ClientThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None, *args, **kwargs):
        threading.Thread.__init__(self)
        self.socket = kwargs['socket']
        self.config = kwargs['config']
        self.logTime = strftime("%d %b %H:%M:%S", gmtime())
        self.identifier = "None"
        r_host = self.config.get('redis', 'host')
        r_port = int(self.config.get('redis', 'port'))
        r_db = int(self.config.get('redis', 'db'))
        self.rcli = redis.Redis(host=r_host, port=r_port, db=r_db)
        self.channel = self.config.get('redis', 'channel')
    
    def log(self, msg):
        print("{logtime}\t{id}\t{msg}".format(logtime=self.logTime, id=self.identifier, msg=msg))
        pass

    def run(self):
        client = self.socket
        if client:
            terminalClient = GPSTerminal(self.socket)
            self.identifier = terminalClient.getIp()
            terminalClient.startReadData()
            if terminalClient.isSuccess():
                self.saveData(terminalClient.getSensorData())
                terminalClient.sendOKClient()
                self.log('Client %s data received successfully!'%terminalClient.getImei())
                print(terminalClient.getSensorData())
            else:
                terminalClient.sendFalse()
                self.log('Client failed')
                pass
            terminalClient.closeConnection()
        else: 
            self.log('Socket is null.')

    def saveData(self, sensorData):
        self.rcli.rpush(self.channel, pickle.dumps(sensorData))

def get_config(config_file):

    config = configparser.RawConfigParser()
    config.add_section('redis')
    config.set('redis', 'channel', 'GPSSensorsData')
    config.set('redis', 'host', 'localhost')
    config.set('redis', 'port', '6379')
    config.set('redis', 'db', '0')

    config.add_section('server')
    config.set('server', 'port', '9980')
    config.read(config_file)
    return config

if __name__ == "__main__":

    optParser = OptionParser()
    optParser.add_option("-c", "--config", dest="conf_file", help="Config file", default="../conf/gps.conf")
    (options, args) = optParser.parse_args()

    config = get_config(options.conf_file)

    print("Gps sensors server. {time}".format(time=strftime("%d %b %H:%M:%S", gmtime())))
    print("Config: {conf_file}".format(conf_file=options.conf_file))
    print("Sensor data db: {redis_host}:{redis_port}/{redis_channel}".format(redis_host=config.get('redis', 'host'), redis_port=config.get('redis', 'port'), redis_channel=config.get('redis', 'channel')))
    print("Server started at port {conf_port}".format(conf_port=int(config.get('server', 'port'))))

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('', int(config.get('server', 'port'))))
    server.listen(5)

    while True:
        ClientThread(socket=server.accept(), config = config).run()
