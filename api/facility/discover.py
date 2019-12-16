from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from config.configuration import sys_log, wisepark_conn, integration_conn
from pathlib import Path
import json
import asyncio


class FacilityDiscover:

    @classmethod
    async def _apply(cls, loop):
        logger = await AsyncLogger().getlogger(sys_log)
        logger.info("Establishing RDBS Wisepark Pool Connection...")
        dbconnector_wp = await AsyncDBPool(size=5, name='discoverer', conn=wisepark_conn, loop=loop)
        logger.info(f"Connected {dbconnector_wp.connected}")
        logger.info("Establishing RDBS Integration Pool Connection...")
        device_mapping_json = str(Path(str(Path(__file__).parents[0]) + "config/devices.json"))
        with open(device_mapping_json) as json_file:
            device_mapping = json.load(json_file)
        dbconnector_is = await AsyncDBPool(size=10, name='discoverer', conn=integration_conn, loop=loop)
        await logger.info(f"Connected {dbconnector_is.connected}")
        if dbconnector_wp.connected and dbconnector_is.connected:
            try:
                devices = await dbconnector_wp.callproc('wp_devices_get', None, [])
                for device in devices:
                    for device_map in device_mapping:
                        if device['terAddress'] == device['terId']:
                            if device['terType'] == 1:
                                await dbconnector_is.callproc('wp_devices_ins', None,
                                                              [device['terAddress'], device['terType'], device['terDirection'],
                                                               device_mapping['ampp_id'], device_mapping['amm_type'], device['terIPV4'],
                                                               device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                               device['terCamPhoto2'], None, None, None, None
                                                               ])
                                codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                                             'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                                             'UpperDoorOpen', 'VoIP', 'Roboticket1', 'Roboticket2', 'TicketReader1',
                                             'TicketReader2', 'IOBoards', 'BarcodeReader1', 'BarcodeReader2',
                                             'Loop1', 'Loop2', '12VBoard', '24VBoard', '24ABoard']
                                for codename in codenames:
                                    await dbconnector_is.callproc('wp_statuses_ins',
                                                                  [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                                                                   codename, '', device['terIPV4']])
                            elif device['terType'] == 2:
                                await dbconnector_is.callproc('wp_devices_ins', None,
                                                              [device['terAddress'], device['terType'], device['terDirection'],
                                                               device_mapping['ampp_id'], device_mapping['amm_type'], device['terIPV4'],
                                                               device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                               device['terCamPhoto2'], None, None, None, None
                                                               ])
                                codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                                             'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                                             'UpperDoorOpen', 'VoIP', 'Roboticket1', 'Roboticket2', 'TicketReader1',
                                             'TicketReader2', 'AlmostOutOfPaper', 'IOBoards', 'PaperDevice1', 'PaperDevice2',
                                             'BarcodeReader1', 'BarcodeReader2', 'TicketPrinter1', 'TicketPrinter2',
                                             'Loop1', 'Loop2', '12VBoard', '24VBoard', '24ABoard']
                                for codename in codenames:
                                    await dbconnector_is.callproc('wp_statuses_ins',
                                                                  [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                                                                   codename, '', device['terIPV4']])
                            elif device['terType'] == 3:
                                await dbconnector_is.callproc('wp_devices_ins', None,
                                                              [device['terAddress'], device['terType'], device['terDirection'],
                                                               device_mapping['ampp_id'], device_mapping['amm_type'], device['terIPV4'],
                                                               None, None, None,
                                                               None, device_mapping.get('imager1', None), device_mapping.get('imager1', None),
                                                               device_mapping.get('payonline', None), device_mapping.get('uniteller', None)
                                                               ])
                                codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                                             'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                                             'UpperDoorOpen', 'VoIP', 'Roboticket1', 'Roboticket2', 'Coinbox', 'CubeHopper', 'CCReader' 'AlmostOutOfPaper',
                                             'IOBoards', 'CoinsReader', 'CoinsHopper1', 'CoinsHopper2', 'CoinsHopper3',
                                             'BarcodeReader1', 'BarcodeReader2', 'TicketPrinter1', 'TicketPrinter2', 'NotesEscrow', 'NotesReader',
                                             'IOCCtalk', 'UPS', 'FiscalPrinter', 'FiscalPrinterBD', 'MiddleDoor', 'CoinBoxTriggered', 'PaperDevice1',
                                             '12VBoard', '24VBoard', '24ABoard']
                                for codename in codenames:
                                    await dbconnector_is.callproc('wp_statuses_ins',
                                                                  [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                                                                   codename, '', device['terIPV4']])
            except Exception as e:
                await logger.error(e)
            finally:
                await logger.shutdown()
                dbconnector_is.terminate()
                dbconnector_wp.terminate()

    @classmethod
    def dispatch(cls):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls._apply(loop))
        loop.close()
