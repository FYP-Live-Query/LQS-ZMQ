package com.mahesh;

import com.mahesh.publisher.Kafka.KafkaMessagePublisher;
import org.apache.tapestry5.json.JSONObject;
import org.zeromq.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mahesh.utils.ZMQBrokerConfig;
import com.mahesh.utils.ZMQBrokerProperties;

import java.util.*;
import java.util.stream.Stream;

public class ZMQServer {
    private static class Context {
        static Logger logger = LoggerFactory.getLogger(ZMQServer.class);
        static ZMQBrokerConfig liveExtensionConfig = new ZMQBrokerConfig.ZMQBrokerConfigBuilder().build();
        static int ZMQServerPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_PORT));
        static Hashtable<String,String> topicsPlusPortsMap = new Hashtable<>();
        private static final HashMap<String, HashMap<String, String>> kafkaTopicsAndRequestedColumnsRoutingTopics = new HashMap<>();
        static int topicPublishingStartingPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_TOPIC_PUBLISHERS_STARTING_PORT));

        static void setupEnvForRequest(byte[] request) {
            JSONObject jsonRequest = new JSONObject(new String(request, ZMQ.CHARSET));
            String kafkaTopic = jsonRequest.getString(ZMQBrokerProperties.KAFKA_TOPIC);
            String kafkaServerHost = jsonRequest.getString(ZMQBrokerProperties.KAFKA_SERVER_HOST);
            boolean columnFilteringEnabled = jsonRequest.getBoolean(ZMQBrokerProperties.COLUMN_NAME_FILTERING_ENABLED);
            List<String> StringColumnNames = new ArrayList<>(100);
            jsonRequest.getJSONArray(ZMQBrokerProperties.COLUMN_NAMES)
                    .forEach(element -> {
                        StringColumnNames.add(String.valueOf(element));
                    });

            String[] sortedStringColumnNames = Stream.of(StringColumnNames.toArray())
                    .sorted()
                    .toArray(String[]::new);

            StringBuilder topicNameForThisListOfColumnNames = new StringBuilder();

            Arrays.stream(sortedStringColumnNames).map(element -> {
                topicNameForThisListOfColumnNames.append(element);
                return null;
            });

            String topicPostFix = UUID.fromString(topicNameForThisListOfColumnNames.toString()).toString();
            if (kafkaTopicsAndRequestedColumnsRoutingTopics.containsKey(kafkaTopic)){
                HashMap<String, String> existingRoutingTopics = kafkaTopicsAndRequestedColumnsRoutingTopics.get(kafkaTopic);
                if (existingRoutingTopics.containsKey(topicNameForThisListOfColumnNames.toString())) {
                    // just return topic name
                } else {
                    existingRoutingTopics.put(
                            topicNameForThisListOfColumnNames.toString(),
                            topicPostFix
                    );
                    // add a filter to publisher and // just return topic name
                }
            } else {
                HashMap<String, String> columnNamesRequested = new HashMap<>();
                columnNamesRequested.put(
                        topicNameForThisListOfColumnNames.toString(), topicPostFix
                );
                kafkaTopicsAndRequestedColumnsRoutingTopics.put(kafkaTopic, columnNamesRequested);
                // create publisher and add a filter to publisher and just return topic name
            }

        }
    }

    public static void main(String[] argv) {
        String banner = "\n\n" +
                ",--,                                                                                                    \n" +
                ",---.'|                                                                                                    \n" +
                "|   | :       ,----..      .--.--.                 ,---,.                          ,-.                     \n" +
                ":   : |      /   /   \\    /  /    '.             ,'  .'  \\                     ,--/ /|                     \n" +
                "|   ' :     /   .     :  |  :  /`. /     ,---,.,---.' .' |  __  ,-.   ,---.  ,--. :/ |             __  ,-. \n" +
                ";   ; '    .   /   ;.  \\ ;  |  |--`    ,'  .' ||   |  |: |,' ,'/ /|  '   ,'\\ :  : ' /            ,' ,'/ /| \n" +
                "'   | |__ .   ;   /  ` ; |  :  ;_    ,---.'   ,:   :  :  /'  | |' | /   /   ||  '  /      ,---.  '  | |' | \n" +
                "|   | :.'|;   |  ; \\ ; |  \\  \\    `. |   |    |:   |    ; |  |   ,'.   ; ,. :'  |  :     /     \\ |  |   ,' \n" +
                "'   :    ;|   :  | ; | '   `----.   \\:   :  .' |   :     \\'  :  /  '   | |: :|  |   \\   /    /  |'  :  /   \n" +
                "|   |  ./ .   |  ' ' ' :   __ \\  \\  |:   |.'   |   |   . ||  | '   '   | .; :'  : |. \\ .    ' / ||  | '    \n" +
                ";   : ;   '   ;  \\; /  |  /  /`--'  /`---'     '   :  '; |;  : |   |   :    ||  | ' \\ \\'   ;   /|;  : |    \n" +
                "|   ,/     \\   \\  ',  . \\'--'.     /           |   |  | ; |  , ;    \\   \\  / '  : |--' '   |  / ||  , ;    \n" +
                "'---'       ;   :      ; | `--'---'            |   :   /   ---'      `----'  ;  |,'    |   :    | ---'     \n" +
                "             \\   \\ .'`--\"                      |   | ,'                      '--'       \\   \\  /           \n" +
                "              `---`                            `----'                                    `----'            \n" +
                "                                                                                                              \n"+
                Context.liveExtensionConfig.getProperty(ZMQBrokerProperties.APPLICATION_TITLE) + " " +
                "v"+Context.liveExtensionConfig.getProperty(ZMQBrokerProperties.APPLICATION_VERSION) + "\n" +
                "Powered by ZeroMQ (TM).\n";

        Context.logger.info(banner);
        Context.logger.info("Starting ZMQServer.");



        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:" + Context.ZMQServerPort);

            Context.logger.info(String.format("Listening on port %s.", Context.ZMQServerPort));
            Context.logger.info(String.format("Now accepting connection on tcp://%s:%s.", Context.liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_IP), Context.ZMQServerPort));

            new Thread(() -> {
                try {
                    while (!Thread.interrupted()) {
                        Thread.sleep(100_000);
                        Iterator<Map.Entry<String, String>> iterator = Context.topicsPlusPortsMap.entrySet().iterator();
                        Context.logger.info("live publishers listing.");
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> kv = iterator.next();
                            Context.logger.info(String.format("[topic : %s] : [port : %s]", kv.getKey(), kv.getValue()));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, "logging-thread-0").start();

            while (!Thread.currentThread().isInterrupted()) {

                byte[] reply = socket.recv(0);
                Context.logger.info("Subscriber connected.");
                Context.logger.info("Received message " + ": [" + new String(reply, ZMQ.CHARSET) + "]");

                String response;

                JSONObject request = new JSONObject(new String(reply, ZMQ.CHARSET));
                String kafkaTopic = request.getString(ZMQBrokerProperties.KAFKA_TOPIC);
                String kafkaServerHost = request.getString(ZMQBrokerProperties.KAFKA_SERVER_HOST);
                boolean columnFilteringEnabled = request.getBoolean(ZMQBrokerProperties.COLUMN_NAME_FILTERING_ENABLED);
                List<String> StringColumnNames = new ArrayList<>(100);
                request.getJSONArray(ZMQBrokerProperties.COLUMN_NAMES)
                        .forEach(element -> {
                            StringColumnNames.add(String.valueOf(element));
                        });
                System.out.println(StringColumnNames);

                if (Context.topicsPlusPortsMap.containsKey(kafkaTopic)) {

                    Context.logger.info(String.format("Kafka Client for Topic '%s' already created ", kafkaTopic));
                    response = request.put(ZMQBrokerProperties.ZMQ_TOPIC_PORT, Context.topicsPlusPortsMap.get(kafkaTopic)).toString();

                } else {

                    Context.topicPublishingStartingPort++;

                    ZThread.fork(
                            context,
                            new KafkaMessagePublisher(
                                    kafkaServerHost,
                                    kafkaTopic,
                                    String.valueOf(Context.topicPublishingStartingPort)));

                    Context.logger.info(
                            String.format("Kafka Client for topic '%s' created and live on port %d.",
                            kafkaTopic,
                                    Context.topicPublishingStartingPort
                            ));

                    Context.topicsPlusPortsMap.put(kafkaTopic, String.valueOf(Context.topicPublishingStartingPort));
                    response = request.put(ZMQBrokerProperties.ZMQ_TOPIC_PORT, String.valueOf(Context.topicPublishingStartingPort)).toString();

                }

                socket.send(response.getBytes(ZMQ.CHARSET), 0);
            }
        }

    }
}
