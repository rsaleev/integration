import configuration as cfg
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool


dbconnector_wp = AsyncDBPool(cfg.wp_cnx, None)
dbconnector_is = AsyncDBPool(cfg.is_cnx, None)
soapconnector_wp = AsyncSOAP(cfg.soap_user, cfg.soap_password, cfg.object_id, cfg.soap_timeout, cfg.soap_url)
logger = AsyncLogger()
amqpconnector = AsyncAMQP(None, cfg.amqp_user, cfg.amqp_password, cfg.amqp_host, 'integration', 'topic')
