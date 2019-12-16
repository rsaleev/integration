from .statuses import *
from dataclasses import dataclass
import datetime


@dataclass
class SNMPObject:

    def __init__(self, codename_value: str, oid_value: str, enum_obj):
        self.codename = codename_value
        self.oid = oid_value
        self.__snmpvalue: int = None
        self.__device_id: int = None
        self.__device_ip: str = None
        self.__device_type: int = None
        self.__statusname = enum_obj
        self.__snmpvalue: int = None
        self.__ampp_id: int = None
        self.__ampp_type: int = None
        self.__ts = datetime

    @property
    def device_id(self):
        return self.__device_id

    @device_id.setter
    def device_id(self, value: int):
        self.__device_id = value

    @device_id.getter
    def device_id(self):
        return self.__device_id

    @property
    def device_type(self):
        return self.__device_type

    @device_type.setter
    def device_type(self, value: int):
        self.__device_type = value

    @device_type.getter
    def device_type(self):
        return self.__device_type

    @property
    def device_ip(self):
        return self.__device_ip

    @device_ip.setter
    def device_ip(self, value: str):
        self.__device_ip = value

    @device_ip.getter
    def device_ip(self):
        return self.__device_ip

    @property
    def ampp_id(self):
        return self.__ampp_id

    @ampp_id.setter
    def ampp_id(self, value):
        self.__ampp_id = value

    @ampp_id.getter
    def ampp_id(self):
        return self.__ampp_id

    @property
    def ampp_type(self):
        return self.__ampp_type

    @ampp_type.setter
    def ampp_type(self, value):
        self.__ampp_type = value

    @ampp_type.getter
    def ampp_type(self):
        return self.__ampp_id

    @property
    def snmpvalue(self):
        return self.__snmpvalue

    @snmpvalue.setter
    def snmpvalue(self, value: int):
        self.__snmpvalue = value

    @snmpvalue.getter
    def snmpvalue(self):
        if not self.__statusname is None:
            return self.__statusname(self.__snmpvalue).name
        else:
            return self.__snmpvalue

    @property
    def ts(self):
        return self.__ts

    @ts.setter
    def ts(self, value: datetime):
        self.__ts = value

    @ts.getter
    def ts(self):
        return self.__ts

    @property
    def data(self):
        return {'device_id': self.device_id,
                'device_type': self.device_type,
                'codename': self.codename,
                'value': self.snmpvalue,
                'ts': self.ts,
                'ampp_id': self.ampp_id,
                'ampp_type': self.ampp_type,
                'device_ip': self.device_ip}

    @property
    def instance(self):
        return self


receiving_mibs = [
    SNMPObject('OutOfService', '.1.3.6.1.4.1.40383.1.2.2.90', None).instance,
    SNMPObject('UpperDoor', '.1.3.6.1.4.1.40383.1.2.2.21', None).instance,
    SNMPObject('RoboticketOOP', '.1.3.6.1.4.1.40383.1.2.1.10001.3.2', None).instance,
    SNMPObject('IOReboot', '.1.3.6.1.4.1.40383.1.2.2.29', None).instance,
    SNMPObject('HotTemperatureWarning', '.1.3.6.1.4.1.40383.1.2.2.86', None).instance,
    SNMPObject('HotTemperatureAlarm', '.1.3.6.1.4.1.40383.1.2.2.88', None).instance,
    SNMPObject('HighHumidityAlarm', '.1.3.6.1.4.1.40383.1.2.2.65', None).instance,
    SNMPObject('ZebraOutOfPaper', '.1.3.6.1.4.1.40383.1.2.1.10002.2.3', None).instance,
    SNMPObject('BarrierStatus', '.1.3.6.1.4.1.40383.1.2.2.111', None).instance,
    SNMPObject('BarrierLoop1Status', '.1.3.6.1.4.1.40383.1.2.2.112', None).instance,
    SNMPObject('BarrierLoop2Status', '.1.3.6.1.4.1.40383.1.2.2.113', None).instance,
    SNMPObject('MiddleDoorOpened', '.1.3.6.1.4.1.40383.1.2.2.16', None).instance,
    SNMPObject('ReceiptPrinterOutOfPaper', '.1.3.6.1.4.1.40383.1.2.1.10004.2.7', None).instance,
    SNMPObject('ActivatedUPS', '.1.3.6.1.4.1.40383.1.2.2.22', None).instance,
    SNMPObject('QuakeAlarm', '1.3.6.1.4.1.40383.1.2.2.12', None).instance,
    SNMPObject('MaxCoinsInSmarthopper', '.1.3.6.1.4.1.40383.1.2.2.24', None).instance,
    SNMPObject('MaxCoinsInBox', '.1.3.6.1.4.1.40383.1.2.2.27', None).instance,
    SNMPObject('MaxCoinsInHopper2', '.1.3.6.1.4.1.40383.1.2.2.25', None).instance,
    SNMPObject('MaxCoinsInHopper3', '.1.3.6.1.4.1.40383.1.2.2.26', None).instance,
    SNMPObject('MaxNotesInSmartpayout', '.1.3.6.1.4.1.40383.1.2.2.28', None).instance,
    SNMPObject('CarReversed', '.1.3.6.1.4.1.40383.1.2.3.4', None).instance,
    SNMPObject('FiscalPrintingError', '.1.3.6.1.4.1.40383.1.2.1.11003.2.1', FiscalPrintingError).instance,
    SNMPObject('FiscalOutOfPaper', '.1.3.6.1.4.1.40383.1.2.1.11003.2.2', None).instance,
    SNMPObject('FiscalMemoryFull', '.1.3.6.1.4.1.40383.1.2.1.11003.2.3', None).instance,
    SNMPObject('PayoutJammedNote', '.1.3.6.1.4.1.40383.1.2.2.116', None).instance,
    SNMPObject('PaymentStatus', '.1.3.6.1.4.1.40383.1.2.3.3', None).instance,
    SNMPObject('PaymentMoneyType', '.1.3.6.1.4.1.40383.1.2.3.1', PaymentType).instance,
    SNMPObject('PaymentStatus', '.1.3.6.1.4.1.40383.1.2.3.3', None).instance,
    SNMPObject('PaymentCardType', '.1.3.6.1.4.1.40383.1.2.3.2', PaymentCardType).instance,
    SNMPObject('PaymentAmount', '.1.3.6.1.4.1.40383.1.2.3.5', None).instance
]

polling_mibs = [
    SNMPObject('General', '.1.3.6.1.4.1.40383.1.2.3.0', General).instance,
    SNMPObject('Heater', '.1.3.6.1.4.1.40383.1.2.2.42', Heater).instance,
    SNMPObject('FanIn', '.1.3.6.1.4.1.40383.1.2.2.40', FanIn).instance,
    SNMPObject('FanOut', '.1.3.6.1.4.1.40383.1.2.2.41', FanOut).instance,
    SNMPObject('IOBoard1.Hummidity', '.1.3.6.1.4.1.40383.1.2.2.65', None).instance,
    SNMPObject('IOBoard2.Humidity', '.1.3.6.1.4.1.40383.1.2.2.75', None).instance,
    SNMPObject('IOBoard1.Temperature', '.1.3.6.1.4.1.40383.1.2.2.64', None).instance,
    SNMPObject('IOBoard2.Temperature', '.1.3.6.1.4.1.40383.1.2.2.74', None).instance,
    SNMPObject('UpperDoor', '.1.3.6.1.4.1.40383.1.2.2.21', UppperDoor).instance,
    SNMPObject('VoIP', '.1.3.6.1.4.1.40383.1.2.1.60001.1', VoIP).instance,
    SNMPObject('Roboticket1', '.1.3.6.1.4.1.40383.1.2.1.10001.2', Roboticket).instance,
    SNMPObject('Roboticket2', '.1.3.6.1.4.1.40383.1.2.1.10006.2', Roboticket).instance,
    SNMPObject('TicketReader1', '.1.3.6.1.4.1.40383.1.2.1.20001.2', TicketReader).instance,
    SNMPObject('TicketReader2', '.1.3.6.1.4.1.40383.1.2.1.20005.2', TicketReader).instance,
    SNMPObject('AlmostOutOfPaper', '1.3.6.1.4.1.40383.1.2.2.0', AlmostOutOfPaper).instance,
    SNMPObject('IOBoards', '.1.3.6.1.4.1.40383.1.2.1.50001.1', IOBoards).instance,
    SNMPObject('PaperDevice1', '.1.3.6.1.4.1.40383.1.2.3.2', PaperDevice).instance,
    SNMPObject('PaperDevice2', '.1.3.6.1.4.1.40383.1.2.3.3', PaperDevice).instance,
    SNMPObject('BarcodeReader1', '.1.3.6.1.4.1.40383.1.2.1.20002.1', BarcodeReader).instance,
    SNMPObject('BarcodeReader2', '.1.3.6.1.4.1.40383.1.2.1.20006.1', BarcodeReader).instance,
    SNMPObject('TicketPrinter1', '.1.3.6.1.4.1.40383.1.2.1.10011.2', TicketPrinter).instance,
    SNMPObject('TicketPrinter2', '.1.3.6.1.4.1.40383.1.2.1.10012.2', TicketPrinter).instance,
    SNMPObject('Coinbox', '.1.3.6.1.4.1.40383.1.2.1.41003.2', Coinbox).instance,
    SNMPObject('CubeHopper', '1.3.6.1.4.1.40383.1.2.1.41002.1', CubeHopper).instance,
    SNMPObject('CCReader', '.1.3.6.1.4.1.40383.1.2.1.30001.1', CCReader).instance,
    SNMPObject('CoinsReader', '.1.3.6.1.4.1.40383.1.2.1.41001.1', CoinsReader).instance,
    SNMPObject('CoinsHopper1', '.1.3.6.1.4.1.40383.1.2.1.41101.1', CoinsHopper).instance,
    SNMPObject('NotesEscrow', '.1.3.6.1.4.1.40383.1.2.1.42101.1', NotesEscrow).instance,
    SNMPObject('NotesReader', '.1.3.6.1.4.1.40383.1.2.1.42001.1', NotesReader).instance,
    SNMPObject('CoinsHopper2', '.1.3.6.1.4.1.40383.1.2.1.41102.1', CoinsHopper).instance,
    SNMPObject('CoinsHopper3', '.1.3.6.1.4.1.40383.1.2.1.41103.1', CoinsHopper).instance,
    SNMPObject('CoinBoxTriggered', '.1.3.6.1.4.1.40383.1.2.2.10', CoinBoxTriggered).instance,
    SNMPObject('MiddleDoor', '.1.3.6.1.4.1.40383.1.2.2.16', MiddleDoor).instance,
    SNMPObject('UPS', '.1.3.6.1.4.1.40383.1.2.2.22', Ups).instance,
    SNMPObject('IOCCtalk', '.1.3.6.1.4.1.40383.1.2.1.50002.1', IOCCTalk).instance,
    SNMPObject('FiscalPrinter', '.1.3.6.1.4.1.40383.1.2.1.10005.1', FiscalPrinter).instance,
    SNMPObject('FiscalPrinterBD', '.1.3.6.1.4.1.40383.1.2.1.10005.2', FiscalPrinterBD).instance,
    SNMPObject('Loop1', '.1.3.6.1.4.1.40383.1.2.2.112', Loop).instance,
    SNMPObject('Loop2', '.1.3.6.1.4.1.40383.1.2.2.113', Loop).instance,
    SNMPObject('12VBoard', '.1.3.6.1.4.1.40383.1.2.2.69', None).instance,
    SNMPObject('24VBoard', '.1.3.6.1.4.1.40383.1.2.2.70', None).instance,
    SNMPObject('24ABoard', '.1.3.6.1.4.1.40383.1.2.2.71', None).instance
]
