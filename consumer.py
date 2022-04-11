from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.exchange_type import ExchangeType
from pydantic import AmqpDsn


class Consumer:
    url: AmqpDsn
    queue_name: str
    exchange_name: str
    exchange_type: ExchangeType
    prefetch_count: int

    def __init__(self) -> None:
        self._connection = None
        self._channel = None
        self._consumer_tag = None
        self._closing = False
        self._consuming = False

    def run(self) -> None:
        """
        Method for running consuming messages.
        """
        self._connection = self._get_connection()
        self._connection.ioloop.start()

    def _get_connection(self) -> SelectConnection:
        """
        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the _on_connection_open method will be invoked by pika.
        """
        return SelectConnection(
            parameters=URLParameters(self.url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )

    def _on_connection_open(self, _connection: SelectConnection) -> None:
        """
        This method is called by pika once the connection to RabbitMQ has been established.
        It passes the handle to the connection object in case we need it.
        """
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: Channel) -> None:
        """
        This method is invoked by pika when the channel has been opened.
        The channel object is passed in, so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use and some callbacks on errors.
        """
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(exchange=self.exchange_name, durable=True, exchange_type=self.exchange_type)
        self._channel.queue_declare(queue=self.queue_name, auto_delete=True)
        self._channel.queue_bind(self.queue_name, self.exchange_name)
        self._channel.basic_qos(prefetch_count=self.prefetch_count)
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.process,
            queue=self.queue_name,
            exclusive=True,
            auto_ack=True,
        )
        self._consuming = True

    def on_consumer_cancelled(self) -> None:
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer receiving messages.
        """
        if self._channel:
            self._channel.close()
            self._consuming = False

    def on_channel_closed(self, _channel: Channel, exception: Exception) -> None:
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that violates the protocol,
        such as re-declare an exchange or queue with different parameters.
        In this case, we'll close the connection to shut down the object.
        """
        logger.error(f'on_channel_closed: {exception}')
        if not (self._connection.is_closing or self._connection.is_closed):
            self._connection.close()

    def on_connection_open_error(self, _connection: SelectConnection, exception: Exception) -> None:
        """
        This method is called by pika if the connection to RabbitMQ can't be established.
        """
        logger.error(f'on_connection_open_error: {exception}')
        self.stop()

    def on_connection_closed(self, _connection: SelectConnection, exception: Exception) -> None:
        """
        This method is invoked by pika when the connection to RabbitMQ is closed unexpectedly.
        Since it is unexpected, we will reconnect to RabbitMQ if it disconnects.
        """
        logger.error(f'on_connection_closed exception: {exception}')
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.reconnect()

    def reconnect(self) -> None:
        """
        Will be invoked if the connection can't be opened or is closed.
        Indicates that a reconnect is necessary then stops the ioloop.
        """
        self.stop()
        self.run()

    def stop(self) -> None:
        """
        Cleanly shutdown the connection to RabbitMQ by stopping the consumer with RabbitMQ.
        """
        if not self._closing:
            self._closing = True
            if self._consuming:
                if self._channel:
                    self._channel.basic_cancel(self._consumer_tag, self.on_consumer_cancelled)
            else:
                self._connection.ioloop.stop()

    def process(self, _channel, basic_deliver, properties, body) -> None:
        """
        Message handler. Implement this in class inheritor for using.
        """
        raise NotImplementedError('Not implemented method on_message_callback in %s.' % self.__class__.__name__)