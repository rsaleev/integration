import time
from datetime import datetime
from threading import Thread
from configuration import session_interval, sql_polling, sys_log, ampp_adapter, epp_adapter
from utils.customlogger import RootLogger
from utils.dbpool import DBPools
from facility.config import ParkingFacility


class PaymentsListener:
    worker = Thread()
    logger = RootLogger(name=__name__, logfile=sys_log)

    payments_data_stmt = """SELECT payIdTerminal as terminal_id, payDateTime as pay_datetime, payType as pay_type, 
                traCode as came_code, traBadge as sub_number, payAmount as pay_amount, payPayed as pay_paid, 
                tidTraKey as ticket_number, payCreation as pay_creation, payCardType as pay_card_type, 
                payUID as pay_uid, payTarId as pay_tariff, traPlate as car_plate, payChange as pay_change
                    FROM paypayment 
                    LEFT OUTER JOIN movtransit
                    ON payTraUID = traUID 
                    LEFT OUTER JOIN movtransitid 
                    ON tidTraId = traId
                    WHERE payCreation > %s 
                    AND payEnabled = 1
                    AND payAmount > 0
                    AND payPayed > 0
                    ORDER BY payCreation;"""

    # last processed record
    last_processed_stmt = """SELECT timestamp FROM facility_processed WHERE operation = 'payment';"""

    # update last processed
    last_processed_upd_stmt = """UPDATE facility_processed SET timestamp = %s WHERE operation = 'payment';"""

    @classmethod
    def _payments_data(cls, ampp_queue, epp_queue):
        while True:
            try:
                last_processed = DBPools.integration.execute(
                    cls.last_processed_stmt, fetch=True, all=False)
                payments_data = DBPools.wisepark.execute(
                    cls.payments_data_stmt, last_processed['timestamp'], fetch=True, all=True)
                if not payments_data is None and len(payments_data) > 0:
                    for payment in payments_data:
                        payment['terminal_id'] = ParkingFacility.map_id(
                            payment['terminal_id'])
                        if ampp_adapter:
                            ampp_queue.put(payment)
                        if epp_adapter and payment['pay_type'] != 'M' and payment['pay_tariff'] != 2:
                            epp_queue.put(payment)
                        DBPools.integration.execute(
                            cls.last_processed_upd_stmt, payment['pay_creation'], fetch=False)
            except Exception as e:
                cls.logger.exception(e)
                continue
            finally:
                time.sleep(sql_polling)

    @classmethod
    def setup(cls, ampp_queue, epp_queue):
        cls.worker = Thread(target=cls._payments_data, args=(
            ampp_queue, epp_queue), daemon=True)

    @classmethod
    def start(cls):
        try:
            cls.worker.start()
        except Exception as e:
            cls.logger.exception(e)
