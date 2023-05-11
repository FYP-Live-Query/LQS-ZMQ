package com.mahesh.publisher.BinLog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.mahesh.publisher.IPublisher;
import io.debezium.common.annotation.Incubating;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.json.JSONObject;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
@Incubating
@InterfaceStability.Unstable
public class BinLogMessagePublisher implements IPublisher
{
    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {
        ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
        publisher.bind("tcp://*:6000");
        Map<Long, String> ordinalPositionAndColumnName  = new HashMap<>();
        String hostName = "10.8.100.246";
        String dbName = "inventory";
        String userName = "root";
        String password = "debezium";
        String port = "3306";
        String jdbcUrl = "jdbc:mysql://"+ userName + ":" + password + "@" + hostName + ":" + port + "/" + dbName;
        Connection connection;
        try {
            connection = DriverManager.getConnection(jdbcUrl, userName, password);
            Statement statement = connection.createStatement();

            String query = "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'networkTraffic'";
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                Long ordinalPosition = Long.parseLong(rs.getString("ORDINAL_POSITION"));
                ordinalPositionAndColumnName.put(ordinalPosition, columnName);
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
//            System.out.println(event);

            JSONObject jsonValue = new JSONObject();
            if(event.getHeader().getEventType().equals(EventType.EXT_WRITE_ROWS)) {
                WriteRowsEventData eventData = event.getData();
                eventData.getRows().stream().forEach(
                        (rowAsArr) -> {
                            IntStream.range(0, rowAsArr.length)
                                    .forEach(index -> {
                                        if(rowAsArr[index] == null){

                                        }else {
                                            jsonValue.put(ordinalPositionAndColumnName.get((long) index + 1), rowAsArr[index]);
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


    }
}
