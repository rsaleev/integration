import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool
import os
import toml


config = toml.load(cs.CONFIG_FILE)

DBCONNECTOR_WS = AsyncDBPool(config['wisepark']['rdbs']['host'],
                             config['wisepark']['rdbs'].get('port', 3306),
                             config['wisepark']['rdbs']['login'],
                             config['wisepark']['rdbs']['password'],
                             config['wisepark']['rdbs']['database'])
DBCONNECTOR_IS = AsyncDBPool(config['integration']['rdbs']['host'],
                             config['integration']['rdbs'].get('port', 3306),
                             config['integration']['rdbs']['login'],
                             config['integration']['rdbs']['password'],
                             config['integration']['rdbs']['database'])
SOAPCONNECTOR = AsyncSOAP(config['wisepark']['soap']['login'],
                          config['wisepark']['soap']['password'],
                          config['wisepark']['site_id'],
                          config['wisepark']['soap']['timeout'],
                          config['wisepark']['soap']['url'])
AMQPCONNECTOR = AsyncAMQP(config['integration']['amqp']['login'],
                          config['integration']['amqp']['password'],
                          config['integration']['amqp']['host'],
                          config['integration']['amqp']['exchange'],
                          config['integration']['amqp']['exchange_type'])
LOGGER = AsyncLogger(f'{cs.LOG_PATH}/integration.log')
