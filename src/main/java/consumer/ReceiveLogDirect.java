package consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ReceiveLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    private Connection connection;
    private Channel channel;

    public ReceiveLogDirect() {
    }

    public void createReceiver(String name, DeliverCallback deliverCallback) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "error");

        System.out.println(name + "- [*] Waiting for messages. To exit press CTRL+C");

        boolean autoAck = true; // acknowledgment is covered below
        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> {
        });
    }

    public void closeReceiver() throws IOException, TimeoutException {
        this.channel.close();
        this.connection.close();
    }
}
