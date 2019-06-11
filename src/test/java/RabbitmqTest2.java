import com.rabbitmq.client.*;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@RunWith(VertxUnitRunner.class)
public class RabbitmqTest2 {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqTest2.class);
    private static final String EXCHANGE_NAME = "direct_logs";

    private static Vertx vertx;

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    @BeforeClass
    public static void setUp() {
        vertx = Vertx.vertx();
    }

    @Before
    public void tearUp(io.vertx.ext.unit.TestContext ctx) throws IOException, TimeoutException {
        TestContext context = new TestContext(ctx);
        vertx.exceptionHandler(context.exceptionHandler());

        this.factory = new ConnectionFactory();
        this.factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
        this.channel.close();
        this.connection.close();
    }

    @Test
    public void test5(io.vertx.ext.unit.TestContext ctx) throws IOException, TimeoutException {
        TestContext context = new TestContext(ctx);
        Async async = context.async();

        createReceiver("test5", (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            logger.debug("test5 - [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            context.assertEquals("direct_log 2", message);
            logger.debug("test5 - [x] Done");
            async.complete();
        });

        createSenderAndSendMessage();

        async.awaitSuccess();
    }

    private void createReceiver(String name, DeliverCallback deliverCallback) throws IOException, TimeoutException {
        this.channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = this.channel.queueDeclare().getQueue();
        this.channel.queueBind(queueName, EXCHANGE_NAME, "error");

        logger.debug(name + "- [*] Waiting for messages. To exit press CTRL+C");

        this.channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private void createSenderAndSendMessage() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String message = "direct_log 2";
            String routingKey = "error";

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
            logger.debug(" [x] Sent '" + routingKey + "':'" + message + "'");
        }
    }

}
