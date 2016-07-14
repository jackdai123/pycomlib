import pika

class Consumer(object):
	def __init__(self, host='localhost', port=5672, username='guest', password='guest', heartbeat_interval=3, connection_attempts=9999, retry_delay=1, socket_timeout=3,
			exchange='exchange', exchange_type='topic', exchange_durable=True, exchange_auto_delete=False,
			queue='queue', queue_durable=True, queue_auto_delete=False, queue_ttl=0, routing_key='#', no_ack=True, handle_message=None):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.heartbeat_interval = heartbeat_interval
		self.connection_attempts = connection_attempts
		self.retry_delay = retry_delay
		self.socket_timeout = socket_timeout
		self.exchange = exchange
		self.exchange_type = exchange_type
		self.exchange_durable = exchange_durable
		self.exchange_auto_delete = exchange_auto_delete
		self.queue = queue
		self.queue_durable = queue_durable
		self.queue_auto_delete = queue_auto_delete
		self.queue_ttl = queue_ttl
		self.routing_key = routing_key
		self.no_ack = no_ack
		self.handle_message = handle_message
		self._connection = None
		self._channel = None
		self._closing = False
		self._consumer_tag = None

	def connect(self):
		parameters = pika.ConnectionParameters(
				host = self.host,
				port = self.port,
				credentials = pika.PlainCredentials(self.username, self.password),
				heartbeat_interval = self.heartbeat_interval,
				connection_attempts = self.connection_attempts,
				retry_delay = self.retry_delay,
				socket_timeout= self.socket_timeout)
		return pika.SelectConnection(parameters, self.on_connection_open, stop_ioloop_on_close=False)

	def on_connection_open(self, unused_connection):
		self.add_on_connection_close_callback()
		self.open_channel()

	def add_on_connection_close_callback(self):
		self._connection.add_on_close_callback(self.on_connection_closed)

	def on_connection_closed(self, connection, reply_code, reply_text):
		self._channel = None
		if self._closing:
			self._connection.ioloop.stop()
		else:
			self._connection.add_timeout(5, self.reconnect)

	def reconnect(self):
		self._connection.ioloop.stop()
		if not self._closing:
			self._connection = self.connect()
			self._connection.ioloop.start()

	def open_channel(self):
		self._connection.channel(on_open_callback=self.on_channel_open)

	def on_channel_open(self, channel):
		self._channel = channel
		self.add_on_channel_close_callback()
		self.setup_exchange()

	def add_on_channel_close_callback(self):
		self._channel.add_on_close_callback(self.on_channel_closed)

	def on_channel_closed(self, channel, reply_code, reply_text):
		self._connection.close()

	def setup_exchange(self):
		self._channel.exchange_declare(
				callback = self.on_exchange_declareok,
				exchange = self.exchange,
				exchange_type = self.exchange_type,
				durable = self.exchange_durable,
				auto_delete = self.exchange_auto_delete)

	def on_exchange_declareok(self, unused_frame):
		self.setup_queue()

	def setup_queue(self):
		arguments = {}
		if self.queue_ttl > 0:
			arguments['x-message-ttl'] = self.queue_ttl * 1000
		self._channel.queue_declare(
				callback = self.on_queue_declareok,
				queue = self.queue,
				durable = self.queue_durable,
				auto_delete = self.queue_auto_delete,
				arguments = arguments)

	def on_queue_declareok(self, method_frame):
		self._channel.queue_bind(
				callback = self.on_bindok,
				queue = self.queue,
				exchange = self.exchange,
				routing_key = self.routing_key)

	def on_bindok(self, unused_frame):
		self.start_consuming()

	def start_consuming(self):
		self.add_on_cancel_callback()
		self._consumer_tag = self._channel.basic_consume(
				consumer_callback = self.on_message,
				queue = self.queue,
				no_ack = self.no_ack)

	def add_on_cancel_callback(self):
		self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

	def on_consumer_cancelled(self, method_frame):
		if self._channel:
			self._channel.close()

	def on_message(self, unused_channel, basic_deliver, properties, body):
		if self.no_ack == False:
			self.acknowledge_message(basic_deliver.delivery_tag)
		self.handle_message(unused_channel, basic_deliver, properties, body)

	def acknowledge_message(self, delivery_tag):
		self._channel.basic_ack(delivery_tag)

	def stop_consuming(self):
		if self._channel:
			self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

	def on_cancelok(self, unused_frame):
		self.close_channel()

	def close_channel(self):
		if self._channel:
			self._channel.close()

	def run(self):
		self._connection = self.connect()
		self._connection.ioloop.start()

	def stop(self):
		self._closing = True
		self.stop_consuming()
		self._connection.ioloop.start()

	def close_connection(self):
		self._connection.close()

def process_value(unused_channel, basic_deliver, properties, body):
	print body
	
def main():
	value_consumer = Consumer(exchange='fss_exchange', queue='storage.value', queue_ttl=600, routing_key='fss.value.#', handle_message=process_value)
	try:
		value_consumer.run()
	except KeyboardInterrupt:
		value_consumer.stop()

if __name__ == '__main__':
	main()
