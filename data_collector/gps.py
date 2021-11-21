import struct
import binascii
import redis
from datetime import datetime
from crc import crc16
import socket


def unpack(fmt, data):
    try:
        return struct.unpack(fmt, data)
    except struct.error:
        flen = struct.calcsize(fmt.replace('*', ''))
        alen = len(data)
        idx = fmt.find('*')
        before_char = fmt[idx-1]
        n = (alen-flen)/struct.calcsize(before_char)+1
        fmt = ''.join((fmt[:idx-1], str(n), before_char, fmt[idx+1:]))
        return struct.unpack(fmt, data)


class GPSTerminal:
    def __init__(self, socket):
        self.socket = socket[0]
        self.ip = socket[1][0]
        self.socket.settimeout(15)
        self.initVariables()

    def initVariables(self):
        self.imei = "unknown"
        self.sensorsDataBlocks = []
        self.error = []
        self.blockCount = 0
        self.success = True
        # Break in data, that was read from socket
        self.dataBreak = 0
        # If we have more than 5 try to read, connection not proceeded
        self.possibleBreakCount = 5
        #More on: https://wiki.teltonika-gps.com/view/How_to_start_with_FMB_devices_and_Sensors%3F#Parsing_of_Beacon_records
        self.beaconFlagsArray = [0x21,  # iBeacon with RSSI
                                 0x23,  # iBeacon with RSSI, Battery Voltage
                                 0x27,  # iBeacon with RSSi, Battery Voltage, Temperature
                                 0x01,  # Eddystone with RSSI
                                 0x03,  # Eddystone with RSSI, Battery Voltage
                                 0x07]  # Eddystone with RSSi, Battery Voltage, Temperature

    def startReadData(self):
        try:
            self.proceedConnection()
        except socket.timeout as err:
            self.error.append("Socket timeout error: %s" % err)
            self.success = False

    def proceedConnection(self):
        if self.isCorrectConnection():
            self.readIMEI()
            if self.imei:
                self.proceedData()
            else:
                self.error.append("Can't read IMEI")
        else:
            self.error.append("Incorrect connection data stream")
            self.success = False

    def proceedData(self):
        """
        Received and proceed work data of GPS Terminal
        """
        self.time = datetime.now()
        self.data = self.readData()
        # Test data
        #self.data = bytes.fromhex("00000000000004DB8E100000017D42F3C2390003400A9A1EA4D1F5001E009E05000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010AC60000017D42F437690003400DAA1EA4CE92001E006906000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010AD20000017D42F4A4C80003400A471EA4D076001C01150600000000000C000500EF0100F00100150500C800004501000500B5000F00B6000E0042370E00430F9C00440000000200F100004FC0001000000997000000000000017D42F4AC990003400A471EA4D076001C011506000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010ACC0000017D42F521C9000340113E1EA4D065001C011506000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010AD30000017D42F539380003400A791EA4D044001A01090600060000000C000500EF0100F00100150500C800004501000500B5000A00B600080042370E00430F9C00440000000200F100004FC00010000009B2000000000000017D42F55878000340090A1EA4D12D001B00FF0500060000000C000500EF0100F00100150500C800004501000500B5001200B600110042370E00430F9C00440000000200F100004FC00010000009B6000000000000017D42F56FE800034007381EA4D12D001D012B0400070000000C000500EF0100F00100150500C800004501000500B5001300B600110042370E00430F9C00440000000200F100004FC00010000009B9000000000000017D42F600700003400D561EA4D46E001E00400400080000000C000500EF0100F00100150500C800004501000500B5001300B600110042370E00430F9C00440000000200F100004FC00010000009C8000000000000017D42F60C290003400E2F1EA4D3E9001E016004000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010ACD0000017D42F6372000034012381EA4D46E001C00520400060000000C000500EF0100F00100150500C800004501000500B5001300B600110042370D00430F9C00440000000200F100004FC00010000009D1000000000000017D42F6566000034016F91EA4D60F001C00380400060000000C000500EF0100F00100150500C800004501000500B5001300B600110042370E00430F9C00440000000200F100004FC00010000009DA000000000000017D42F66DD000034018681EA4D60F001B01620400060000000C000500EF0100F00100150500C800004501000500B5001300B600110042370E00430F9C00440000000200F100004FC00010000009DE000000000000017D42F6815900034016E81EA4D526001A005E05000001810001000000000000000000010181001711210102030405060708090A0B0C0D0E0F10020B010ACA0000017D42F6B03800034015261EA4D4B1001A004A0400070000000C000500EF0100F00100150500C800004501000500B5001200B600110042370A00430F9C00440000000200F100004FC00010000009E7000000000000017D42F6EEB80003401A5B1EA4D673001700380400060000000C000500EF0100F00100150500C800004501000500B5001200B600110042370E00430F9C00440000000200F100004FC00010000009F1000000001000006F0C")

        if self.data:
            Hexline = binascii.hexlify(self.data)
            self.Hexline = Hexline.decode()
            self.AVL = 0  # AVL - Pointer to the data =bits
            # Extract the first 8 bits which are zeros
            Zeros = self.extract(8)
            # Extract the AVL Length, the NEXT 8 bits
            AVLLength = self.extract(8)
            # Identify the CODEC, the next 2 bits, in each step the AVL increases by the number
            # specified in the extract
            # TODO: Maybe make a class that uses a different way of decoding
            CodecID = self.extract(2)
            # Count the AVL Data packs, we need to go through all the AVL data packs to
            # decode the infomration
            BlockCount = self.extract_int(2)
            self.blockCount = BlockCount
            # The amount of block covered so far
            proceed = 0
            # The position of the current AVL Block to be decoded
            AVLBlockPos = 0

            while proceed < self.blockCount:
                try:
                    data = self.proceedBlockData()
                    print(data)
                    self.sensorsDataBlocks.append(data)
                except ValueError as e:
                    self.error.append("Value error: %s" % e)
                    print("Value error: %s" % e)
                    self.dataBreak += 1
                    # In case data consistency problem, we are re-trying to read data from socket
                    self.reReadData(Hexline)
                    # If we have more than possibleBreakCount problems, stop reading
                    if self.dataBreak > self.possibleBreakCount:
                        # After one year we have 0 problem trackers, and +200k that successfully send data after more than one break
                        self.error.append("Data break")
                        self.success = False
                        return
                    else:
                        self.AVL = AVLBlockPos
                        # Re try read data from current block
                        proceed -= 1
                proceed += 1
                AVLBlockPos = self.AVL
        else:
            self.error.append("No data received")
            self.success = False

    def readData(self, length=8192):
        data = self.socket.recv(length)
        return data

    def reReadData(self, Hexline):
        HexlineNew = unpack("s*", self.readData())
        Hexline += binascii.hexlify(HexlineNew[0])
        self.Hexline = Hexline

    def proceedBlockData(self):
        """
        Proceed block data from received data
        """
        # Timestamp is regitered as 16 bits
        # Why 0x? https://stackoverflow.com/a/209550
        DateV = '0x' + self.extract(16)
        DateS = int(round(int(DateV, 16) / 1000, 0))
        Prio = self.extract_int(2)
        GpsLng = self.extract_int(8)
        GpsLat = self.extract_int(8)
        Lng = str(float(GpsLng)/10000000)
        Lat = str(float(GpsLat)/10000000)
        Alt = self.extract_int(4)
        Course = self.extract_int(4)
        Sats = self.extract_int(2)
        Speed = self.extract_int(4)
        IOEventCode = self.extract_int(4)
        NumOfIO = self.extract_int(4)

        sensorDataResult = {}
        pais_count = 0

        # FIXME: I am not sure if the last value 16 will the correctnes of this program properly
        # The last bit can be a variable size which can be problematic for the program
        # Right now is under the assumption that the value is fixed
        if IOEventCode != 385:
            arr = [1, 2, 4, 8, 16]
        else:
            # -1 indicates that the last entry is a beacon and must be handed differently
            # it's not the most ideal solution, rather a hacky fix
            arr = [1, 2, 4, 8, -1]

        for i in arr:
            pc = 0
            data = self.readSensorDataBytes(i)
            for iocode in data.keys():
                pais_count += 1
                sensorDataResult[iocode] = data[iocode]
                pc += 1

        return {'imei': self.imei, 'date': DateS, 'lng': Lng, 'lat': Lat, 'alt': Alt, 'course': Course, 'sats': Sats, 'speed': Speed, 'sensorData': sensorDataResult}

    def readSensorDataBytes(self, count):
        result = {}
        pairsCount = self.extract_int(4)
        if count != -1:
            i = 1
            while i <= pairsCount:
                IOCode = self.extract_int(4)
                IOVal = self.extract_int(count * 2)
                result[IOCode] = IOVal
                i += 1
        else:
            IOCode = self.extract_int(4)
            self.extract(4)  # There is some type of header here (0017)
            # Data part
            # (First half byte – current data part, Second half byte – total number of data parts)
            # I don't know what these indicate so far pricesly, the reference website was not very clear on these
            # Reference website: https://wiki.teltonika-gps.com/view/How_to_start_with_FMB_devices_and_Sensors%3F#Parsing_of_Beacon_records
            currentDataPart = self.extract_int(1)
            dataParts = self.extract_int(1)
            beaconFlag = self.extract_int(2)
            beaconResult = {}
            count = 0
            while (beaconFlag in self.beaconFlagsArray):
                if beaconFlag == self.beaconFlagsArray[0] or beaconFlag == self.beaconFlagsArray[1] or beaconFlag == self.beaconFlagsArray[2]:
                    UIID = self.extract(32)
                    minor = self.extract(4)
                    major = self.extract(4)
                    signal = self.twos_complement(hex(self.extract_int(2)), 8)
                    battery = "Not supported"
                    temperature = "Not supported"
                    if beaconFlag == self.beaconFlagsArray[1]:
                        battery = self.extract_int(4)
                    if beaconFlag == self.beaconFlagsArray[2]:
                        battery = self.extract_int(4)
                        temperature = self.extract_int(4)
                    beaconResult[count] = {'UUID': UIID, 'Minor': minor, 'Major': major,
                                           'Signal': signal, 'Battery': battery, 'Temperature': temperature}
                elif beaconFlag == self.beaconFlagsArray[3] or beaconFlag == self.beaconFlagsArray[4] or beaconFlag == self.beaconFlagsArray[5]:
                    # I am just skipping over these values and not taking them into account for now
                    namespace = self.extract(20)
                    insatnceId = self.extract(12)
                    signal = self.twos_complement(hex(self.extract_int(2)), 8)
                    battery = "Not supported"
                    temperature = "Not supported"
                    if beaconFlag == self.beaconFlagsArray[4]:
                        battery = self.extract_int(4)
                    if beaconFlag == self.beaconFlagsArray[5]:
                        battery = self.extract_int(4)
                        temperature = self.extract_int(4)
                count += 1
                return beaconResult
        return result

    def extract(self, length):
        result = self.Hexline[self.AVL: (self.AVL + length)]
        self.AVL += length
        return result

    def extract_int(self, length):
        hex_number = self.extract(length)
        return int(hex_number, 16)

    # https://stackoverflow.com/questions/6727875/hex-string-to-signed-int-in-python-3-2
    def twos_complement(self, hexstr, bits):
        value = int(hexstr, 16)
        if value & (1 << (bits-1)):
            value -= 1 << bits
        return value

    def readIMEI(self):
        IMEI = self.readData(34)
        self.imei = IMEI.decode()
        self.socket.send(chr(1).encode())

    def isCorrectConnection(self):
        """
        Check data from client terminal for correct first bytes
        """
        hello = self.readData(2)
        return '(15,)' == str(
            struct.unpack("!H", hello)
        )

    def sendOKClient(self):
        """
        Reply for connected client that data correctly received
        """
        self.socket.send(struct.pack("!L", self.blockCount))
        self.closeConnection()

    def sendFalse(self):
        self.socket.send(struct.pack("!L", 0))
        self.closeConnection()

    def closeConnection(self):
        # self.socket.close()
        p = 1

    def getSensorData(self):
        return self.sensorsDataBlocks

    def getIp(self):
        return self.ip

    def getImei(self):
        return self.imei

    def isSuccess(self):
        return self.success


#GPSTerminal(None)
