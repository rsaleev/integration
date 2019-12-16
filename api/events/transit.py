from dataclasses import dataclass
from datetime import datetime


@dataclass
class Entry:
    def __init__(self):
        self.ticket_number = int
        self.ticket_code = int
        self.enabled = int
        self.car_plate = str
        self.start_time = datetime
        self.stop_time = datetime
        self.terminal_id = int
        self.area = int
        self.type = str
        self.tariff = int
        self.lost = bool

    @property
    def event(self, data: dict):


@dataclass
class Exit:
    def __init__(self):
        self.ticket_number = int
        self.ticket_code = int
        self.enabled = int
        self.car_plate = str
        self.start_time = datetime
        self.stop_time = datetime
        self.terminal_id = int
        self.area = int
        self.type = str
        self.tariff = int
        self.paid = bool

    @property
    def event(self, data: dict):
        self.ticket_number = data['tidTraKey']
        self.ticket_code = data['traCode']
        self.enabled = data['traEnabled']
        self.car_plate = data['traPlate']
        self.start_time 
        if data['traBadge'] != '':
            self.type = 'subscription'

        """SELECT traUID, traCreation, traDisableTime, traCode, traPlate, 
        traEnabled, traType, traIdTerminal, traBadge, traTarId, 
        (SELECT COUNT(payPayed) FROM paypayment WHERE payTraUID = traUID) as traPayments),
        (SELECT tidTraKey FROM movtransitid 
        LEFT OUTER JOIN  movtransit
        ON tidTraId = traId
        WHERE traCode = traCode 
        AND traDirection ='I')
        WHERE traAreId = 1
        AND traDisableTime <> '0000-00-00 00:00:00'
        AND traCreation > %s
        AND traDirection = 'O'
        AND traEnabled = 0
        ORDER BY traCreation;"""

        # SELECT traCreation as start_time, traCode as came_code,
        # traDisableTime as stop_time, traPlate as car_plate, traDirection as car_direction,
        # traEnabled as enabled, traType as entrytype, traIdTerminal as tra_terminal,  traBadge as sub_number,
        # tidTraKey as ticket_number, traTarId as tariff, terType as ter_type, (SELECT cusSurname as sub_number
        #                         FROM tabcustomer
        #                         LEFT OUTER JOIN abbcards
        #                         ON carSubId=cusUseId
        #                         WHERE carCode=traCode)
        #         FROM movtransit
        #         LEFT OUTER JOIN  movtransitid
        #         ON tidTraId = traId
        #         LEFT OUTER JOIN tabterminal
        #         ON traIdTerminal = terId
        #         WHERE traAreId = 1
        #         AND traDisableTime = '0000-00-00 00:00:00'
        #         AND traCreation > %s
        #         AND traDirection = 'O'
        #         AND traEnabled = 1
        #         ORDER BY traCreation;"""
