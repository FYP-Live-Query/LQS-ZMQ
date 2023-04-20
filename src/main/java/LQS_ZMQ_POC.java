import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
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
            subscriber.subscribe("networkTraffic".getBytes(ZMQ.CHARSET));

            int count = 0;
            while (true) {
                String string = subscriber.recvStr();
                System.out.println(string);
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
            Map<Long, String> longTableMapEventDataMap  = new HashMap<>();
            String hostName = "10.8.100.246";
            String dbName = "inventory";
            String userName = "root";
            String password = "debezium";
            String port = "3306";
            String jdbcUrl = "jdbc:mysql://" + hostName + ":" + port + "/" + dbName;
            Connection connection;
            try {
                connection = DriverManager.getConnection(jdbcUrl, userName, password);
                Statement statement = connection.createStatement();

                String query = "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'networkTraffic'";
                ResultSet rs = statement.executeQuery(query);
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    Long ordinalPosition = Long.parseLong(rs.getString("ORDINAL_POSITION"));
                    longTableMapEventDataMap.put(ordinalPosition, columnName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


            BinaryLogClient client = new BinaryLogClient("10.8.100.246", 3306,"inventory", "root", "debezium");
            EventDeserializer eventDeserializer = new EventDeserializer();

            eventDeserializer.setCompatibilityMode(
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
            );
            client.setEventDeserializer(eventDeserializer);

            client.registerEventListener(event -> {

                JSONObject jsonValue = new JSONObject();
                if(event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS)) {
                    WriteRowsEventData eventData = event.getData();
                    eventData.getRows().stream().forEach(
                            (rowAsArr) -> {
                                IntStream.range(0, rowAsArr.length)
                                        .forEach(index -> {
                                            if(rowAsArr[index] == null){

                                            }else {
                                                jsonValue.put(longTableMapEventDataMap.get((long) index + 1), rowAsArr[index]);
                                            }
                                        });
                            }
                    );
                    jsonValue.put("initial_data", "false"); // as required by the backend processing
                    JSONObject obj = new JSONObject();
                    obj.put("properties", jsonValue); // all user required data for siddhi processing inside properties section in JSON object
                    String strMsg = obj.toString();
                    String string = String.format("%s-%s", "networkTraffic" , strMsg);
                    if (!publisher.send(string)) {
                        System.exit(0); //  Interrupted
                    }
                    System.out.println(System.currentTimeMillis() - (long) (eventData.getRows().get(0)[5]));

                }
            });
            try {
                client.connect();
            } catch (IOException e) {
                e.printStackTrace();
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
//                ZFrame frame = ZFrame.recvFrame(pipe);
//                if (frame == null)
//                    break; //  Interrupted
////                frame.print(null);
//                frame.destroy();
            }
        }
    }

    //  .split main thread
    //  The main task starts the subscriber and publisher, and then sets
    //  itself up as a listening proxy. The listener runs as a child thread:
    public static void main(String[] argv) throws IOException, SQLException {
        try (ZContext ctx = new ZContext()) {
            //  Start child threads
            ZThread.fork(ctx, new Publisher());
            ZThread.fork(ctx, new Subscriber());

            Socket subscriber = ctx.createSocket(SocketType.XSUB);
            subscriber.connect("tcp://localhost:6000");
            Socket publisher = ctx.createSocket(SocketType.XPUB);
            publisher.bind("tcp://*:6001");
            Socket listener = ZThread.fork(ctx, new Listener());
            ZMQ.proxy(subscriber, publisher, listener);

            System.out.println(" interrupted");
        }

    }
}
