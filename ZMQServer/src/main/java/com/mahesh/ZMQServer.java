package com.mahesh;

import com.mahesh.publisher.Kafka.KafkaMessagePublisher;
import org.apache.tapestry5.json.JSONObject;
import org.zeromq.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mahesh.utils.ZMQBrokerConfig;
import com.mahesh.utils.ZMQBrokerProperties;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class ZMQServer {

    public static void main(String[] argv) {

        ZMQBrokerConfig liveExtensionConfig = new ZMQBrokerConfig.ZMQBrokerConfigBuilder().build();

        Logger logger = LoggerFactory.getLogger(ZMQServer.class);
        logger.info("Starting ZMQServer.");

        int ZMQServerPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_PORT));
        Hashtable<String,String> topicsPlusPortsMap = new Hashtable<>();
        int topicPublishingStartingPort = Integer.parseInt(liveExtensionConfig.getProperty(ZMQBrokerProperties.ZMQ_TOPIC_PUBLISHERS_STARTING_PORT));

        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:" + ZMQServerPort);

            logger.info(String.format("Listening on port %s.", ZMQServerPort));
            logger.info(String.format("Now accepting connection on tcp://%s:%s.", ZMQBrokerProperties.ZMQ_BROKER_SERVER_HOST_IP, ZMQServerPort));

            new Thread(() -> {
                try {
                    while (!Thread.interrupted()) {
                        Thread.sleep(100000);
                        Iterator<Map.Entry<String, String>> iterator = topicsPlusPortsMap.entrySet().iterator();
                        logger.info("live publishers listing.");
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> kv = iterator.next();
                            logger.info(String.format("[topic : %s] : [port : %s]", kv.getValue(), kv.getKey()));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            while (!Thread.currentThread().isInterrupted()) {

                byte[] reply = socket.recv(0);
                logger.info("Subscriber connected.");
                logger.info("Received message " + ": [" + new String(reply, ZMQ.CHARSET) + "]");
                String response;
                JSONObject request = new JSONObject(new String(reply, ZMQ.CHARSET));

                if (topicsPlusPortsMap.containsKey(request.getString("topic"))) {

                    logger.info(String.format("Topic '%s' already created ", request.getString("topic")));
                    response = request.put("port",topicsPlusPortsMap.get(request.getString("topic"))).toString();

                } else {

                    topicPublishingStartingPort++;
                    ZThread.fork(
                            context,
                            new KafkaMessagePublisher(request.getString("kafka.server.host"),
                                    request.getString("topic"),
                                    String.valueOf(topicPublishingStartingPort)));
                    logger.info(String.format("Publisher for topic '%s' created and live on port %d.", request.getString("topic"), topicPublishingStartingPort));
                    topicsPlusPortsMap.put(request.getString("topic"),String.valueOf(topicPublishingStartingPort));
                    response = request.put("port",String.valueOf(topicPublishingStartingPort)).toString();

                }

                socket.send(response.getBytes(ZMQ.CHARSET), 0);
            }
        }

    }
}
