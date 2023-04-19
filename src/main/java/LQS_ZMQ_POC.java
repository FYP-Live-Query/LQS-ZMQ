import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.json.JSONObject;
import org.zeromq.*;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread.IAttachedRunnable;


public class LQS_ZMQ_POC {
    //  The subscriber thread requests messages starting with
    //  A and B, then reads and counts incoming messages.
    private static class Subscriber implements IAttachedRunnable
    {

        @Override
        public void run(Object[] args, ZContext ctx, Socket pipe)
        {
            //  Subscribe to "A" and "B"
            Socket subscriber = ctx.createSocket(SocketType.SUB);
            subscriber.connect("tcp://localhost:6001");
            subscriber.subscribe("A".getBytes(ZMQ.CHARSET));
            subscriber.subscribe("B".getBytes(ZMQ.CHARSET));
            subscriber.subscribe("C".getBytes(ZMQ.CHARSET));

            int count = 0;
            while (true) {
                String string = subscriber.recvStr();
                if (string == null)
                    break; //  Interrupted
                count++;
            }
            ctx.destroySocket(subscriber);
        }
    }

    //  .split publisher thread
    //  The publisher sends random messages starting with A-J:
    private static class Publisher implements IAttachedRunnable
    {
        @Override
        public void run(Object[] args, ZContext ctx, Socket pipe)
        {
            Socket publisher = ctx.createSocket(SocketType.PUB);
            publisher.bind("tcp://*:6000");
            Random rand = new Random(System.currentTimeMillis());

            while (!Thread.currentThread().isInterrupted()) {
                String string = String.format("%c-%05d", 'A' + rand.nextInt(10), rand.nextInt(100000));
                if (!publisher.send(string))
                    break; //  Interrupted
                try {
                    Thread.sleep(100); //  Wait for 1/10th second
                }
                catch (InterruptedException e) {
                }
            }
            ctx.destroySocket(publisher);
        }
    }

    //  .split listener thread
    //  The listener receives all messages flowing through the proxy, on its
    //  pipe. In CZMQ, the pipe is a pair of ZMQ_PAIR sockets that connect
    //  attached child threads. In other languages your mileage may vary:
    private static class Listener implements IAttachedRunnable
    {
        @Override
        public void run(Object[] args, ZContext ctx, Socket pipe)
        {
            //  Print everything that arrives on pipe
            while (true) {
                ZFrame frame = ZFrame.recvFrame(pipe);
                if (frame == null)
                    break; //  Interrupted
                frame.print(null);
                frame.destroy();
            }
        }
    }

    //  .split main thread
    //  The main task starts the subscriber and publisher, and then sets
    //  itself up as a listening proxy. The listener runs as a child thread:
    public static void main(String[] argv) throws IOException, SQLException {
        try (ZContext ctx = new ZContext()) {
            //  Start child threads
//            ZThread.fork(ctx, new Publisher());
//            ZThread.fork(ctx, new Subscriber());
//
//            Socket subscriber = ctx.createSocket(SocketType.XSUB);
//            subscriber.connect("tcp://localhost:6000");
//            Socket publisher = ctx.createSocket(SocketType.XPUB);
//            publisher.bind("tcp://*:6001");
//            Socket listener = ZThread.fork(ctx, new Listener());
//            ZMQ.proxy(subscriber, publisher, listener);
//
//            System.out.println(" interrupted");

            // NB: child threads exit here when the context is closed

        }
        Map<Long, String> longTableMapEventDataMap  = new HashMap<>();
        String hostName = "10.8.100.246";
        String dbName = "inventory";
        String userName = "root";
        String password = "debezium";
        String port = "3306";
        String jdbcUrl = "jdbc:mysql://" + hostName + ":" + port + "/" + dbName;

        Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
        Statement statement = connection.createStatement();
        // Execute the selectSQL query and process the results

        String query = "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'networkTraffic'";
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()) {
            // Do something with each row
            String columnName = rs.getString("COLUMN_NAME");
            Long ordinalPosition = Long.parseLong(rs.getString("ORDINAL_POSITION"));
            longTableMapEventDataMap.put(ordinalPosition, columnName);
//            System.out.println(columnName +" " +ordinalPosition);
            // ...
        }
        
        BinaryLogClient client = new BinaryLogClient("10.8.100.246", 3306,"inventory", "root", "debezium");
        EventDeserializer eventDeserializer = new EventDeserializer();

        client.setEventDeserializer(eventDeserializer);
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                System.out.println(event);
                if(event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS)) {
                    WriteRowsEventData eventData = event.getData();

                    System.out.println(System.currentTimeMillis() - (long) (eventData.getRows().get(0)[5]));
                }else if(event.getHeader().getEventType().equals(EventType.TABLE_MAP)){
                    TableMapEventData eventData = event.getData();
                    System.out.println(eventData.getColumnMetadata());
                }
            }
        });
        client.connect();
        // Define the configuration for the Debezium Engine with MySQL connector...
//        final Properties props = new Properties();
//        props.setProperty("name", "engine");
//        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
//        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
//        props.setProperty("offset.storage.file.filename", "/tmp/connect.offsets");
//        props.setProperty("offset.flush.interval.ms", "60000");
//        /* begin connector properties */
//        props.setProperty("database.hostname", "10.8.100.246");
//        props.setProperty("database.port", "3306");
//        props.setProperty("database.user", "root");
//        props.setProperty("database.password", "debezium");
//        props.setProperty("database.server.id", "223344");
//        props.setProperty("topic.prefix", "l");
//        props.setProperty("schema.history.internal",
//                "io.debezium.storage.file.history.FileSchemaHistory");
//        props.setProperty("schema.history.internal.file.filename",
//                "/tmp/schemahistory.dat");
//        AtomicLong i = new AtomicLong();
//        AtomicInteger events = new AtomicInteger();
//        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
//                .using(props)
//                .notifying(record -> {
//                    String stringJsonMsg = record.value();
//                    JSONObject jsonObject = new JSONObject(stringJsonMsg);
//                    long newValue = (long) ((JSONObject) ((JSONObject) jsonObject.get("payload")).get("after"))
//                            .get("eventTimestamp");
//                    long l = System.currentTimeMillis() - newValue;
//                    if(l < 1000){
//                        long j = i.addAndGet(l);
//                        events.getAndIncrement();
//                        System.out.println("avg : " + j / events.get());
//                        System.out.println(stringJsonMsg);
//                    }
//
//                    System.out.println(System.currentTimeMillis() - newValue);
//                }).build()
//        ) {
//            // Run the engine asynchronously ...
//            ExecutorService executor = Executors.newSingleThreadExecutor();
//            executor.execute(engine);
//
//            // Do something else or wait for a signal or an event
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }
}
