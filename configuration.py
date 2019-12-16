import os
from configparser import RawConfigParser
from datetime import datetime
from pathlib import Path

config_file = (str(Path(str(Path(__file__).parents[1]) + "/configuration/config.ini")))
parser = RawConfigParser()
parser.read(config_file)


### SQL ###
wp_cnx = {"user": parser.get("WISEPARK", "user"),
          "password": parser.get("WISEPARK", "password"),
          "host": parser.get("WISEPARK", "host"),
          "db": parser.get("WISEPARK", "db"),
          "port": parser.getint("WISEPARK", "port"),
          }
is_cnx = {"user": parser.get("INTEGRATION", "user"),
          "password": parser.get("INTEGRATION", "password"),
          "host": parser.get("INTEGRATION", "host"),
          "db": parser.get("INTEGRATION", "db"),
          "port": parser.getint("INTEGRATION", "port"),
          }

### AMPP DEVICE MAPPING ###
device_mapping = str(Path(str(Path(__file__).parents[1]) + "/configuration/devices.json"))

### LOG FILES ###

if not os.path.isdir(str(Path(str(Path(__file__).parents[1]) + "/logs"))):
    os.mkdir(str(Path(str(Path(__file__).parents[1]) + "/logs")))

sys_log = (str(Path(str(Path(__file__).parents[1]) + "/logs/sys.log")))


# SNMP
snmp_polling = parser.getint("SNMP", "poller_interval")
snmp_timeout = parser.getint("SNMP", "poller_timeout")
snmp_retries = parser.getint("SNMP", "poller_retries")
snmp_trap_host = parser.get("SNMP", "receiver_host")
snmp_trap_port = parser.getint("SNMP", "receiver_port")


# AMQP
amqp_host = parser.get("AMQP", "host")
amqp_port = parser.getint("AMQP", "port")
amqp_user = parser.get("AMQP", "user")
amqp_password = parser.get("AMQP", "password")
