from pika.adapters.blocking_connection import BlockingChannel
from pika import BlockingConnection, ConnectionParameters, PlainCredentials


class RabbitMQConsumerMixin:
    """
    Mixin for getting messages from RabbitMQ when it's needed.
    """
    connection: BlockingConnection
    channel: BlockingChannel
    queue: str

    def rabbit_connect(self, username, password, host, virtual_host, port, queue, exchange):
        """
        Connect to RabbitMQ as BlockingConnection.
        After completion, you can implement self.channel.basic_get() for getting messages.
        Important don't forget about ack messages.
        """
        credentials = PlainCredentials(username=username, password=password)
        connection_parameters = ConnectionParameters(host, port, virtual_host, credentials)

        self.queue = queue
        self.connection = BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.queue, auto_delete=True, exclusive=True)
        self.channel.queue_bind(queue, exchange)

    def get_messages(self, count):  # TODO: add return hint
        """
        Method for getting messages from RabbitMQ.
        """
        return self.channel.basic_get(self.queue, auto_ack=True)