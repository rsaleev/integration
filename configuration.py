import os
from configparser import RawConfigParser
from datetime import datetime
from pathlib import Path

CONFIGURATION = str(Path(str(Path(__file__).parents[1]) + "/configuration/config.ini"))
MAPPING = str(Path(str(Path(__file__).parents[1]) + "/configuration/mapping.json"))
PLACES = str(Path(str(Path(__file__).parents[1]) + "/configuration/places.json"))
TEMP = str(Path(str(Path(__file__).parents[0]) + "/tmp/"))
IMAGES_PATH = str(Path(str(Path(__file__).parents[0]) + "/tmp/photos"))
PLATES = str(Path(str(Path(__file__).parents[1])+"/tmp/plates"))
METRO = str(Path(str(Path(__file__).parents[1])+"/resources/metro"))

parser = RawConfigParser()
parser.read(CONFIGURATION)

### WISEPARK SERVER ###

server_ip = parser.get("WISEPARK", "server_ip")
object_id = parser.get("WISEPARK", "site_id")

main_area = parser.getint("WISEPARK", "main_area")
### WISEPARK SOAP SERVICE ###


object_latitude = parser.getfloat("AMPP", "parking_latitude")
object_longitude = parser.getfloat("AMPP", "parking_longitude")
object_address = parser.get("AMPP", "parking_address")


soap_user = parser.get("WISEPARK", "soap_username")
soap_password = parser.get("WISEPARK", "soap_password")
soap_url = parser.get("WISEPARK", "soap_url")
soap_timeout = parser.getint("WISEPARK", "soap_timeout")

### RDBS ###
is_cnx = {"user": parser.get("INTEGRATION", "rdbs_user"),
          "password": parser.get("INTEGRATION", "rdbs_password"),
          "host": parser.get("INTEGRATION", "rdbs_host"),
          "db": parser.get("INTEGRATION", "rdbs_db"),
          "port": parser.getint("INTEGRATION", "rdbs_port")}


wp_cnx = {"user": parser.get("WISEPARK", "rdbs_user"),
          "password": parser.get("WISEPARK", "rdbs_password"),
          "host": parser.get("WISEPARK", "rdbs_host"),
          "db": parser.get("WISEPARK", "rdbs_db"),
          "port": parser.getint("WISEPARK", "rdbs_port")}


rdbs_polling_interval = parser.getint("WISEPARK", "rdbs_poller_interval")

### AMPP ###
ampp_parking_id = parser.getint("CMIU", "parking_id")

### METRO ###
converter_url = parser.get("METRO", "info_url")
passes_url = parser.get("METRO", "info_url")
metro_timeout = parser.getint('METRO', "timeout")


### LOG FILES ###
if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs")))
log = (str(Path(str(Path(__file__).parents[1]) + "/logs/sys.log")))
log_debug = (str(Path(str(Path(__file__).parents[1]) + "/logs/sys_debug.log")))
# SNMP
snmp_polling = parser.getint("INTEGRATION", "snmp_poller_interval")
snmp_timeout = parser.getint("INTEGRATION", "snmp_poller_timeout")
snmp_retries = parser.getint("INTEGRATION", "snmp_poller_retries")
snmp_trap_host = parser.get("INTEGRATION", "snmp_receiver_host")
snmp_trap_port = parser.getint("INTEGRATION", "snmp_receiver_port")
snmp_debug = parser.getboolean("INTEGRATION", "snmp_debug")

# AMQP
amqp_user = parser.get("INTEGRATION", "amqp_user")
amqp_password = parser.get("INTEGRATION", "amqp_password")
amqp_host = parser.get("INTEGRATION", "amqp_host")

asgi_host = parser.get("INTEGRATION", "asgi_host")
asgi_port = parser.getint("INTEGRATION", "asgi_port")
asgi_workers = parser.getint("INTEGRATION", "asgi_workers")
asgi_log = parser.get("INTEGRATION", "asgi_log")
