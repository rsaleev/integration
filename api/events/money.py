

tra_type
inventory = self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[data['device_id']])
for inv in inventory:
                                    money = self.Money(inv['curTerId'], inv['curChannelId'], inv['curValue'], inv['curQuantity'], inv['curAmount'], data['ts'])
                                    body = json.dumps(money.instance).encode()
                                    asyncio.ensure_future(self.__dbconnector_is.callproc('wp_money_ins', rows=0, values=[
                                        money.terminalid, data['amppId'], money.channel, money.value, money.quantity, money.ts]))
                                    asyncio.ensure_future(self.__amqp_sender_ex.publish(Message(body), routing_key='money', delivery_mode=DeliveryMode.PERSISTENT))
