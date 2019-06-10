import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@RunWith(VertxUnitRunner.class)
public class RabbitmqTest {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqTest.class);

    private static Vertx vertx;

    @BeforeClass
    public static void setUp() {
        VertxOptions options = new VertxOptions();
        options.setBlockedThreadCheckInterval(1000 * 60 * 60);

        vertx = Vertx.vertx(options);
    }

    @Test
    public void test1(io.vertx.ext.unit.TestContext ctx) throws IOException, TimeoutException {
        TestContext context = new TestContext(ctx);
        Async async = context.async();

        final Receiver receiver = new Receiver();
        final Sender sender = new Sender();

        receiver.createReceiver("test1", (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("test1 - [x] Received '" + message + "', tag => " + consumerTag);
            context.assertEquals("Hello World!", message);
            System.out.println("test1 - [x] Done");
            async.complete();
        });

        sender.createSenderAndSendMessage();

        async.awaitSuccess();
    }

    @Test
    public void test2(io.vertx.ext.unit.TestContext ctx) throws IOException, TimeoutException {
        TestContext context = new TestContext(ctx);
        Async async = context.async();

        final NewTask newTask = new NewTask();
        final Worker worker = new Worker();

        worker.createReceiver("test2", (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("test2 - [x] Received '" + message + "', tag => " + consumerTag);

            worker.doWork(message);

            context.assertEquals("new task", message);
            System.out.println("test2 - [x] Done");
            async.complete();
        });

        newTask.createSenderAndSendMessage();

        async.awaitSuccess();
    }

    @Test
    public void test3(io.vertx.ext.unit.TestContext ctx) throws IOException, TimeoutException {
        TestContext context = new TestContext(ctx);
        Async async = context.async();

        final ReceiveLog receiveLog = new ReceiveLog();
        final EmitLog emitLog = new EmitLog();

        receiveLog.createReceiver("test3", (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("test3 - [x] Received '" + message + "', tag => " + consumerTag);
            context.assertEquals("log", message);
            System.out.println("test3 - [x] Done");
            async.complete();
        });

        emitLog.createSenderAndSendMessage();

        async.awaitSuccess();
    }
}
