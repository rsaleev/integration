import configuration as cfg
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool


dbconnector_wp = AsyncDBPool(cfg.wp_cnx)
dbconnector_is = AsyncDBPool(cfg.is_cnx)
soapconnector = AsyncSOAP(cfg.soap_user, cfg.soap_password, cfg.object_id, cfg.soap_timeout, cfg.soap_url)
amqpconnector = AsyncAMQP(cfg.amqp_user, cfg.amqp_password, cfg.amqp_host, 'integration', 'topic')
logger = AsyncLogger()
devices = []
gates = [d for d in devices if d['terType'] in [1, 2]]
autocashiers = [d for d in devices if d['terType'] == 3]



# data producers

