import datetime
from dataclasses import dataclass
from enum import Enum

from integration.producers.snmp.statuses import *


@dataclass
class SNMPObject:

    def __init__(self, codename_value: str, oid_value: str, status_map: object = None, forced_value: str = None):
        self.codename = codename_value
        self.oid = oid_value
        self.__snmpvalue: int = None
        self.__device_id: int = None
        self.__device_address: int = None
        self.__device_ip: str = None
        self.__device_type: int = None
        self.__device_area: int = None
        self.__statusname = status_map
        self.__statusforced = forced_value
        self.__snmpvalue: int = -1
        self.__ampp_id: int = None
        self.__ampp_type: int = None
        self.__ts = datetime
        self.__keyword: str = None
        # default value
        self.__act_uid: str = "00000000-0000-0000-0000-000000000000"
        self.__tra_uid: str = "00000000-0000-0000-0000-000000000000"

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
    def device_address(self):
        return self.__device_address

    @device_address.setter
    def device_address(self, value: int):
        self.__device_address = value

    @device_address.getter
    def device_address(self):
        return self.__device_address

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
        return self.__ampp_type

    @property
    def snmpvalue(self):
        return self.__snmpvalue

    @snmpvalue.setter
    def snmpvalue(self, value):
        if isinstance(value, int):
            self.__snmpvalue = value
        else:
            pass

    @snmpvalue.getter
    def snmpvalue(self):
        if self.__statusname:
            return self.__statusname(self.__snmpvalue).name
        elif self.__statusforced:
            return self.__statusforced
        elif not self.__statusname and not self.__statusforced:
            if self.codename in ["12VBoard", "24VBoard"]:
                return self.__snmpvalue/10
            elif self.codename == "24ABoard":
                return self.__snmpvalue/100
            elif self.codename in ['IOBoard1.Temperature', 'IOBoard1.Temperature']:
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
    def act_uid(self):
        return self.__act_uid

    @act_uid.setter
    def act_uid(self, value):
        self.__act_uid = str(value)

    @act_uid.getter
    def act_uid(self):
        return self.__act_uid

    @property
    def tra_uid(self):
        return self.__act_uid

    @tra_uid.setter
    def tra_uid(self, value):
        self.__tra_uid = str(value)

    @tra_uid.getter
    def tra_uid(self):
        return self.__tra_uid

    @property
    def device_area(self):
        return self.__device_area

    @device_area.setter
    def device_area(self, value):
        self.__device_area = value

    @device_area.getter
    def device_area(self):
        return self.__device_area

    @property
    def data(self):
        return {'device_id': self.device_id,
                'device_address': self.device_address,
                'device_type': self.device_type,
                'device_area': self.device_area,
                'codename': self.codename,
                'value': self.snmpvalue,
                'act_uid': self.act_uid,
                'tra_uid': self.tra_uid,
                'ts': self.ts,
                'ampp_id': self.ampp_id,
                'ampp_type': self.ampp_type,
                'device_ip': self.device_ip}

    @property
    def instance(self):
        return self


receiving_mibs = [
    SNMPObject('General', '.1.3.6.1.4.1.40383.1.2.2.90', forced_value='OUT_OF_SERVICE').instance,
    SNMPObject('General', '.1.3.6.1.4.1.40383.1.2.2.29', forced_value='REBOOTING').instance,
    SNMPObject('UpperDoor', '.1.3.6.1.4.1.40383.1.2.2.21', forced_value='OPENED_WITH_ALARM').instance,
    SNMPObject('Roboticket1', '.1.3.6.1.4.1.40383.1.2.1.10001.3.2', forced_value='OUT_OF_TICKETS').instance,
    SNMPObject('Roboticket2', '.1.3.6.1.4.1.40383.1.2.1.10001.3.2', forced_value='OUT_OF_TICKETS').instance,
    SNMPObject('IOBoard1.Temperature', '.1.3.6.1.4.1.40383.1.2.2.86', forced_value='HOT_TEMP_WARNING').instance,
    SNMPObject('IOBoard1.Temperature', '.1.3.6.1.4.1.40383.1.2.2.88', forced_value='HOT_TEMP_ALARM').instance,
    SNMPObject('IOBoard1.Humidity', '.1.3.6.1.4.1.40383.1.2.2.65', forced_value='HIGH_HUMIDITY_ALARM').instance,
    SNMPObject('TicketPrinter1', '.1.3.6.1.4.1.40383.1.2.1.10002.2.3', forced_value='OUT_OF_TICKETS').instance,
    SNMPObject('BarrierStatus', '.1.3.6.1.4.1.40383.1.2.2.111', Barrier).instance,
    SNMPObject('BarrierLoop1Status', '.1.3.6.1.4.1.40383.1.2.2.112', status_map=Loop).instance,
    SNMPObject('BarrierLoop2Status', '.1.3.6.1.4.1.40383.1.2.2.113', status_map=Loop).instance,
    SNMPObject('BarrierLoop1Reverse', '.1.3.6.1.4.1.40383.1.2.3.4', forced_value='REVERSE').instance,
    SNMPObject('MiddleDoor', '.1.3.6.1.4.1.40383.1.2.2.16', forced_value='OPENED_WITH_ALARM').instance,
    SNMPObject('FiscalPrinterIssues', '.1.3.6.1.4.1.40383.1.2.1.10004.2.7', forced_value='OUT_OF_PAPER').instance,
    SNMPObject('UPS', '.1.3.6.1.4.1.40383.1.2.2.22', forced_value='EXTERNAL_SOURCE_IS_DOWN').instance,
    SNMPObject('General', '1.3.6.1.4.1.40383.1.2.2.12', forced_value='QUAKE_ALARM').instance,
    SNMPObject('CoinsHopper1', '.1.3.6.1.4.1.40383.1.2.2.24', forced_value='MAX_COINS').instance,
    SNMPObject('Coinbox', '.1.3.6.1.4.1.40383.1.2.2.27', forced_value='MAX_COINS').instance,
    SNMPObject('CoinsHopper2', '.1.3.6.1.4.1.40383.1.2.2.25', forced_value='MAX_COINS').instance,
    SNMPObject('CoinsHopper3', '.1.3.6.1.4.1.40383.1.2.2.26', forced_value='MAX_COINS').instance,
    SNMPObject('NotesEscrow', '.1.3.6.1.4.1.40383.1.2.2.28', forced_value='MAX_NOTES').instance,
    SNMPObject('FiscalPrinterIssues', '.1.3.6.1.4.1.40383.1.2.1.11003.2.1', status_map=FiscalPrintingError).instance,
    SNMPObject('FiscalPrinterIssues', '.1.3.6.1.4.1.40383.1.2.1.11003.2.2', forced_value='ALMOST_OUT_OF_PAPER').instance,
    SNMPObject('FiscalPrinterBD', '.1.3.6.1.4.1.40383.1.2.1.11003.2.3', forced_value='MEMORY_IS_FULL').instance,
    SNMPObject('SmartPayout', '.1.3.6.1.4.1.40383.1.2.2.116', forced_value='JAMMED_NOTE').instance,
    SNMPObject('PaymentStatus', '.1.3.6.1.4.1.40383.1.2.3.3', status_map=PaymentStatus).instance,
    SNMPObject('PaymentType', '.1.3.6.1.4.1.40383.1.2.3.1', status_map=PaymentType).instance,
    SNMPObject('PaymentCardType', '.1.3.6.1.4.1.40383.1.2.3.2', status_map=PaymentCardType).instance,
    SNMPObject('PaymentAmount', '.1.3.6.1.4.1.40383.1.2.3.5').instance,
]

polling_mibs = [
    SNMPObject('General', '.1.3.6.1.4.1.40383.1.2.3.0', status_map=General).instance,
    SNMPObject('Heater', '.1.3.6.1.4.1.40383.1.2.2.42', status_map=Heater).instance,
    SNMPObject('FanIn', '.1.3.6.1.4.1.40383.1.2.2.40', status_map=FanIn).instance,
    SNMPObject('FanOut', '.1.3.6.1.4.1.40383.1.2.2.41', status_map=FanOut).instance,
    SNMPObject('UpperDoor', '.1.3.6.1.4.1.40383.1.2.2.21', status_map=UppperDoor).instance,
    SNMPObject('VoIP', '.1.3.6.1.4.1.40383.1.2.1.60001.1', status_map=VoIP).instance,
    SNMPObject('Roboticket1', '.1.3.6.1.4.1.40383.1.2.1.10001.2', status_map=Roboticket).instance,
    SNMPObject('Roboticket2', '.1.3.6.1.4.1.40383.1.2.1.10006.2', status_map=Roboticket).instance,
    SNMPObject('AlmostOutOfPaper', '.1.3.6.1.4.1.40383.1.2.2.0', status_map=AlmostOutOfPaper).instance,
    SNMPObject('IOBoards', '.1.3.6.1.4.1.40383.1.2.1.50001.1', status_map=IOBoards).instance,
    SNMPObject('PaperDevice1', '.1.3.6.1.4.1.40383.1.2.3.2', status_map=PaperDevice).instance,
    SNMPObject('PaperDevice2', '.1.3.6.1.4.1.40383.1.2.3.3', status_map=PaperDevice).instance,
    SNMPObject('IOBoard1.Temperature', '.1.3.6.1.4.1.40383.1.2.2.64').instance,
    SNMPObject('IOBoard2.Temperature', '.1.3.6.1.4.1.40383.1.2.2.74').instance,
    SNMPObject('VoIP', '.1.3.6.1.4.1.40383.1.2.1.60001.1', status_map=VoIP).instance,
    SNMPObject('TicketReader1', '.1.3.6.1.4.1.40383.1.2.1.20001.2', status_map=TicketReader).instance,
    SNMPObject('TicketReader2', '.1.3.6.1.4.1.40383.1.2.1.20005.2', status_map=TicketReader).instance,
    SNMPObject('BarcodeReader1', '.1.3.6.1.4.1.40383.1.2.1.20002.1', status_map=BarcodeReader).instance,
    SNMPObject('BarcodeReader2', '.1.3.6.1.4.1.40383.1.2.1.20006.1', status_map=BarcodeReader).instance,
    SNMPObject('TicketPrinter1', '.1.3.6.1.4.1.40383.1.2.1.10011.2', status_map=TicketPrinter).instance,
    SNMPObject('TicketPrinter2', '.1.3.6.1.4.1.40383.1.2.1.10012.2', status_map=TicketPrinter).instance,
    SNMPObject('Coinbox', '.1.3.6.1.4.1.40383.1.2.1.41003.2', status_map=Coinbox).instance,
    SNMPObject('CubeHopper', '.1.3.6.1.4.1.40383.1.2.1.41002.1', status_map=CubeHopper).instance,
    SNMPObject('CCReader', '.1.3.6.1.4.1.40383.1.2.1.30001.1', status_map=CCReader).instance,
    SNMPObject('CoinsReader', '.1.3.6.1.4.1.40383.1.2.1.41001.1', status_map=CoinsReader).instance,
    SNMPObject('CoinsHopper1', '.1.3.6.1.4.1.40383.1.2.1.41101.1', status_map=CoinsHopper).instance,
    SNMPObject('NotesEscrow', '.1.3.6.1.4.1.40383.1.2.1.42101.1', status_map=NotesEscrow).instance,
    SNMPObject('NotesReader', '.1.3.6.1.4.1.40383.1.2.1.42001.2', status_map=NotesReader).instance,
    SNMPObject('CoinsHopper2', '.1.3.6.1.4.1.40383.1.2.1.41102.1', status_map=CoinsHopper).instance,
    SNMPObject('CoinsHopper3', '.1.3.6.1.4.1.40383.1.2.1.41103.1', status_map=CoinsHopper).instance,
    SNMPObject('CoinBoxTriggered', '.1.3.6.1.4.1.40383.1.2.2.10', status_map=CoinBoxTriggered).instance,
    SNMPObject('MiddleDoor', '.1.3.6.1.4.1.40383.1.2.2.16', status_map=MiddleDoor).instance,
    SNMPObject('UPS', '.1.3.6.1.4.1.40383.1.2.2.22', status_map=Ups).instance,
    SNMPObject('IOCCtalk', '.1.3.6.1.4.1.40383.1.2.1.50002.1', status_map=IOCCTalk).instance,
    SNMPObject('FiscalPrinterStatus', '.1.3.6.1.4.1.40383.1.2.1.10005.1', status_map=FiscalPrinter).instance,
    SNMPObject('FiscalPrinterBD', '.1.3.6.1.4.1.40383.1.2.1.10005.2', status_map=FiscalPrinterBD).instance,
    SNMPObject('BarrierStatus', '.1.3.6.1.4.1.40383.1.2.2.2', status_map=Barrier).instance,
    SNMPObject('BarrierLoop1Status', '.1.3.6.1.4.1.40383.1.2.2.18', status_map=Loop).instance,
    SNMPObject('BarrierLoop2Status', '.1.3.6.1.4.1.40383.1.2.2.19', status_map=Loop).instance,
    SNMPObject('BarrierLoop3Status', '.1.3.6.1.4.1.40383.1.2.2.20', status_map=Loop).instance,
    SNMPObject('12VBoard', '.1.3.6.1.4.1.40383.1.2.2.69').instance,
    SNMPObject('24VBoard', '.1.3.6.1.4.1.40383.1.2.2.70').instance,
    SNMPObject('24ABoard', '.1.3.6.1.4.1.40383.1.2.2.71').instance,
]
