from datetime import datetime
from threading import Thread
import time
from configuration import sql_polling, sys_log, area_id, session_interval
from facility.config import ParkingFacility
from utils.customlogger import RootLogger
from utils.dbpool import DBPools


class MoneyListener:
    worker = Thread()
    logger = RootLogger(name=__name__, logfile=sys_log)

    money_data_stmt = """SELECT curTerId as terminal_id, curType as currency_type, curValue as currency_value, 
        curQuantity as currency_qty, curChannelId as channel_id, payCreation as pay_creation
         FROM paycurinventory
         WHERE payCreation > %s
         ORDER BY payCreation;"""

    # last processed record
    last_processed_stmt = """SELECT timestamp FROM facility_processed WHERE operation = 'money';"""

    # update last processed
    last_processed_upd_stmt = """UPDATE facility_processed SET timestamp = %s WHERE operation = 'money';"""

    @classmethod
    def _money_data(cls, q):
        while True:
            try:
                last_processed = DBPools.integration.execute(
                    cls.last_processed_stmt, fetch=True, all=False)
                money_data = DBPools.wisepark.execute(
                    cls.money_data_stmt, last_processed['timestamp'], fetch=True, all=True)
                if not money_data is None and len(money_data) > 0:
                    for money in money_data:
                        money['terminal_id'] = ParkingFacility.map_id(
                            money['terminal_id'])
                        q.put(money)
                        DBPools.integration.execute(
                            cls.last_processed_upd_stmt, money['pay_creation'], fetch=False)
            except Exception as e:
                cls.logger.exception(e)
                continue
            finally:
                time.sleep(sql_polling)

    @classmethod
    def setup(cls, q):
        cls.worker = Thread(target=cls._money_data, args=(q,), daemon=True)

    @classmethod
    def start(cls):
        try:
            cls.worker.start()
        except Exception as e:
            cls.logger.exception(e)
