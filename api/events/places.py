import time
from threading import Thread

from configuration import sys_log, sql_polling
from facility.config import ParkingFacility
from utils.customlogger import RootLogger
from utils.dbpool import DBPools
from datetime import datetime

class PlacesListener:
    workers = []
    logger = RootLogger(name=__name__, logfile=sys_log)

    @classmethod
    def _get(cls):
        while True:
            try:
                if ParkingFacility.places.subs_total >0:
                    subs = DBPools.wisepark.execute("""SELECT CAST(SUM(argNrInside) AS SIGNED) as occupied, 
                                                        CAST(SUM(argMaxInside) - SUM(argNrInside)AS SIGNED) as free
                                                        FROM abbareagroup;""", fetch=True, all=False)
                    ParkingFacility.places.subs_occupied = subs["occupied"]
                    ParkingFacility.places.subs_free = subs["free"]
                if ParkingFacility.places.comm_total > 0:
                    comm = DBPools.wisepark.execute("""SELECT CAST(areTotalPark - areFreePark) as occupied, areFreePark as free FROM tabarea;""", fetch=True,all=False)
                    ParkingFacility.places.comm_occupied = comm["occupied"] - ParkingFacility.places.physchal_occupied-ParkingFacility.places.subs_occupied
                    ParkingFacility.places.comm_free = comm["free"] - ParkingFacility.places.physchal_free - ParkingFacility.places.subs_free
            except Exception as e:
                cls.logger.exception(e)
                continue
            finally:
                time.sleep(sql_polling)

    @classmethod
    def _update(cls):
        while True:
            try:
                total_free = ParkingFacility.places.comm_free + \
                    ParkingFacility.places.physchal_free+ParkingFacility.places.subs_free
                DBPools.wisepark.execute("""UPDATE tabarea SET areFreePark = %s;""", total_free)
            except Exception as e:
                cls.logger.exception(e)
                continue
            finally:
                time.sleep(sql_polling)

    @classmethod
    def setup(cls):
        cls.get_places = Thread(target=cls._get, name="places_listener", daemon=True)
        cls.workers.append(cls.get_places)
        cls.update_places = Thread(target=cls._update, name="places_updater", daemon=True)
        cls.workers.append(cls.update_places)

    @classmethod
    def start(cls):
        for worker in cls.workers:
            try:
                worker.start()
            except Exception as e:
                cls.logger.error(
                    f"An exception occurred while starting thread {worker.name}")
            finally:
                if worker.isAlive():
                    cls.logger.info(f"Thread {worker.name}")
                else:
                    cls.logger.error(f"Couldn't start thread {worker.name}")
