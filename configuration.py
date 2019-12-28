import os
from configparser import RawConfigParser
from datetime import datetime
from pathlib import Path

config_file = (str(Path(str(Path(__file__).parents[1]) + "/configuration/config.ini")))
parser = RawConfigParser()
parser.read(config_file)

### WISEPARK SERVER ###

server_ip = parser.get("WISEPARK", "server_ip")
### RDBS ###
wp_cnx = {"user": parser.get("WISEPARK", "rdbs_user"),
          "password": parser.get("WISEPARK", "rdbs_password"),
          "host": parser.get("WISEPARK", "rdbs_host"),
          "db": parser.get("WISEPARK", "rdbs_db"),
          "port": parser.getint("WISEPARK", "rdbs_port"),
          }
is_cnx = {"user": parser.get("INTEGRATION", "rdbs_user"),
          "password": parser.get("INTEGRATION", "rdbs_password"),
          "host": parser.get("INTEGRATION", "rdbs_host"),
          "db": parser.get("INTEGRATION", "rdbs_db"),
          "port": parser.getint("INTEGRATION", "rdbs_port"),
          }

rdbs_polling_interval = parser.getint("WISEPARK", "rdbs_poller_interval")
rdbs_polling_from = parser.get("AMPP", "start_date")


### AMPP ###
device_mapping = str(Path(str(Path(__file__).parents[1]) + "/configuration/devices.json"))
ampp_parking_id = parser.getint("AMPP", "parking_id")
physically_challenged_total = parser.getint("AMPP", "physically_challenged_places")


### LOG FILES ###
if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs")))
if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs/integration"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs/integration")))
sys_log = (str(Path(str(Path(__file__).parents[1]) + "/logs/integration/sys.log")))

# SNMP
snmp_polling = parser.getint("INTEGRATION", "snmp_poller_interval")
snmp_timeout = parser.getint("INTEGRATION", "snmp_poller_timeout")
snmp_retries = parser.getint("INTEGRATION", "snmp_poller_retries")
snmp_trap_host = parser.get("INTEGRATION", "snmp_receiver_host")
snmp_trap_port = parser.getint("INTEGRATION", "snmp_receiver_port")


amqp_user = parser.get("INTEGRATION", "amqp_user")
amqp_password = parser.get("INTEGRATION", "amqp_password")
amqp_host = parser.get("INTEGRATION", "amqp_host")
