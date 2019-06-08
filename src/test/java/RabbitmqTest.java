import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.unit.Async;
import io.vertx.reactivex.ext.unit.TestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@RunWith(VertxUnitRunner.class)
public class RabbitmqTest {

    private static final Logger logger = LoggerFactory.getLogger(RabbitmqTest.class);

    private static Vertx vertx;

    @Before
    public void setUp(){
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

        receiver.createReceiver((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            context.assertEquals("Hello World!", message);
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

        worker.createReceiver((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            try {
                Worker.doWork(message);
                async.complete();

            } catch (InterruptedException e) {
                context.fail();
            } finally {
                System.out.println(" [x] Done");
            }
            context.assertEquals("new task", message);
        });

        newTask.createSenderAndSendMessage();

        async.awaitSuccess();
    }
}
