package com.mahesh;

import com.mahesh.publisher.Kafka.KafkaMessagePublisher;
import org.apache.tapestry5.json.JSONArray;
import org.apache.tapestry5.json.JSONObject;
import org.zeromq.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mahesh.utils.ZMQBrokerConfig;
import com.mahesh.utils.ZMQBrokerProperties;

import java.util.*;

public class ZMQServer {

    public static void main(String[] argv) {

        Logger logger = LoggerFactory.getLogger(ZMQServer.class);
        ZMQBrokerConfig liveExtensionConfig = new ZMQBrokerConfig.ZMQBrokerConfigBuilder().build();

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
                liveExtensionConfig.getProperty(ZMQBrokerProperties.APPLICATION_TITLE) + " " +
                "v"+liveExtensionConfig.getProperty(ZMQBrokerProperties.APPLICATION_VERSION) + "\n" +
                "Powered by ZeroMQ (TM).\n";

        logger.info(banner);
        logger.info("Starting ZMQServer.");

        int ZMQServerPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_PORT));
        Hashtable<String,String> topicsPlusPortsMap = new Hashtable<>();
        int topicPublishingStartingPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_TOPIC_PUBLISHERS_STARTING_PORT));

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:" + ZMQServerPort);

            logger.info(String.format("Listening on port %s.", ZMQServerPort));
            logger.info(String.format("Now accepting connection on tcp://%s:%s.", liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_IP), ZMQServerPort));

            new Thread(() -> {
                try {
                    while (!Thread.interrupted()) {
                        Thread.sleep(100_000);
                        Iterator<Map.Entry<String, String>> iterator = topicsPlusPortsMap.entrySet().iterator();
                        logger.info("live publishers listing.");
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> kv = iterator.next();
                            logger.info(String.format("[topic : %s] : [port : %s]", kv.getKey(), kv.getValue()));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, "logging-thread-0").start();

            while (!Thread.currentThread().isInterrupted()) {

                byte[] reply = socket.recv(0);
                logger.info("Subscriber connected.");
                logger.info("Received message " + ": [" + new String(reply, ZMQ.CHARSET) + "]");

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

                if (topicsPlusPortsMap.containsKey(kafkaTopic)) {

                    logger.info(String.format("Kafka Client for Topic '%s' already created ", kafkaTopic));
                    response = request.put(ZMQBrokerProperties.ZMQ_TOPIC_PORT, topicsPlusPortsMap.get(kafkaTopic)).toString();

                } else {

                    topicPublishingStartingPort++;

                    ZThread.fork(
                            context,
                            new KafkaMessagePublisher(
                                    kafkaServerHost,
                                    kafkaTopic,
                                    String.valueOf(topicPublishingStartingPort)));

                    logger.info(
                            String.format("Kafka Client for topic '%s' created and live on port %d.",
                            kafkaTopic,
                            topicPublishingStartingPort
                            ));

                    topicsPlusPortsMap.put(kafkaTopic, String.valueOf(topicPublishingStartingPort));
                    response = request.put(ZMQBrokerProperties.ZMQ_TOPIC_PORT, String.valueOf(topicPublishingStartingPort)).toString();

                }

                socket.send(response.getBytes(ZMQ.CHARSET), 0);
            }
        }

    }
}
