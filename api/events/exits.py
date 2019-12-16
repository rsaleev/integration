import time
from datetime import datetime
from threading import Thread
from configuration import sql_polling, sys_log, ampp_adapter, epp_adapter, sf_adapter, session_interval
from facility.config import ParkingFacility
from utils.customlogger import RootLogger
from utils.dbpool import DBPools


class ExitListener:
    worker = Thread()
    logger = RootLogger(name=__name__, logfile=sys_log)
    # create queue to return data from thread
    # query entry transit data
    movtransit_data_stmt = """SELECT  traCreation as stop_time, traCode as came_code, 
        traDisableTime as stop_time, traPlate as car_plate, traDirection as car_direction, 
        traEnabled as tra_enabled, traType as tra_type, traIdTerminal as tra_terminal,  traBadge as sub_number,  
        tidTraKey as ticket_number, traTarId as tariff
                FROM movtransit
                LEFT OUTER JOIN  movtransitid
                ON tidTraId = traId
                WHERE traAreId = 1
                AND traDisableTime <> '0000-00-00 00:00:00'
                AND traCreation > %s
                AND traDirection = 'O'
                AND traEnabled = 0
                ORDER BY traCreation;"""
    # find AMPP card number by traBadge code
    abbcards_data_stmt = """SELECT cusSurname as ampp_card_num
                                FROM tabcustomer 
                                LEFT OUTER JOIN abbcards
                                ON carId = cusId
                                WHERE carCode = %s;"""
    # find an entry pair
    movtransit_pair_stmt = """SELECT tidTraKey as ticket_number 
                                    FROM movtransitid
                                    LEFT OUTER JOIN movtransit 
                                    ON traId=tidTraId 
                                    WHERE  traDirection='I'
                                    AND traCode =%s;"""

    # last processed record
    last_processed_stmt = """SELECT timestamp FROM facility_processed WHERE operation = 'exit';"""

    # update last processed
    last_processed_upd_stmt = """UPDATE facility_processed SET timestamp = %s WHERE operation = 'exit';"""

    @classmethod
    def _transits_data(cls, ampp_q, epp_q, sf_q):
        # while polling is running change interval after 1st cycle
        while True:
            try:
                # get last processed timestamp
                last_processed = DBPools.integration.execute(
                    cls.last_processed_stmt, fetch=True, all=False)
                exits = DBPools.wisepark.execute(cls.movtransit_data_stmt,
                                                 last_processed['timestamp'],
                                                 fetch=True,
                                                 all=True)
                if not exits is None and len(exits) > 0:
                    for exit in exits:
                        if exit['sub_number'] != '':
                            ampp_code = DBPools.wisepark.execute(cls.abbcards_data, exit['sub_number'], fetch=True,
                                                                 all=False)
                            if not ampp_code is None and len(ampp_code) > 0:
                                exit['sub_number'] = ampp_code["ampp_card_num"]
                        entry_ticket = DBPools.wisepark.execute(cls.movtransit_pair, exit['came_code'], fetch=True,
                                                                all=False)
                        if not entry_ticket is None and not entry_ticket["ticket_number"] is None:
                            exit["ticket_number"] = entry_ticket["ticket_number"]
                        exit['tra_terminal'] = ParkingFacility.map_id(
                            exit['tra_terminal'])
                        # store result
                        if ampp_adapter:
                            ampp_q.put(exit)
                        if sf_adapter and exit['sub_number'] == '':
                            sf_q.put(exit)
                        if epp_adapter and exit['sub_number'] == '':
                            epp_q.put(exit)
                        DBPools.integration.execute(
                            cls.last_processed_upd_stmt, exit['stop_time'], fetch=False)
            except Exception as e:
                cls.logger.exception(e)
                continue
            finally:
                time.sleep(sql_polling)

    @classmethod
    def setup(cls, ampp_q, epp_q, sf_q):
        cls.worker = Thread(target=cls._transits_data,
                            args=(ampp_q, epp_q, sf_q), daemon=True)

    @classmethod
    def start(cls):
        try:
            cls.worker.start()
        except Exception as e:
            cls.logger.exception(e)
