import os
from configparser import RawConfigParser
from datetime import datetime
from pathlib import Path

CONFIG_FILE = (str(Path(str(Path(__file__).parents[1]) + "/configuration/config.ini")))
parser = RawConfigParser()
parser.read(CONFIG_FILE)


logger = object


### WISEPARK SERVER ###

server_ip = parser.get("WISEPARK", "server_ip")
object_id = parser.get("WISEPARK", "site_id")


### WISEPARK SOAP SERVICE ###

soap_user = parser.get("WISEPARK", "soap_username")
soap_password = parser.get("WISEPARK", "soap_password")
soap_url = parser.get("WISEPARK", "soap_url")
soap_timeout = parser.getint("WISEPARK", "soap_timeout")

cashbox_limit = parser.getint("WISEPARK", "cashbox_limit")

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
device_mapping = str(Path(str(Path(__file__).parents[1]) + "/configuration/devices.json"))
places_mapping = str(Path(str(Path(__file__).parents[1]) + "/configuration/places.json"))
ampp_parking_id = parser.getint("AMPP", "parking_id")
physically_challenged_total = parser.getint("AMPP", "physically_challenged_places")

### LOG FILES ###
if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs")))
if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs/integration"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs/integration")))
log = (str(Path(str(Path(__file__).parents[1]) + "/logs/integration/sys.log")))
log_debug = (str(Path(str(Path(__file__).parents[1]) + "/logs/integration/sys_debug.log")))
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

asgi_host = parser.get("REMOTE", "asgi_host")
asgi_port = parser.getint("REMOTE", "asgi_port")
asgi_workers = parser.getint("REMOTE", "asgi_workers")
asgi_debug = parser.getboolean("REMOTE", "asgi_debug")
