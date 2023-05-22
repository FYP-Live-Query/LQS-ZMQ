package com.mahesh;

import com.mahesh.publisher.Kafka.KafkaMessagePublisher;
import org.apache.tapestry5.json.JSONObject;
import com.mahesh.publisher.BinLog.BinLogMessagePublisher;
import org.zeromq.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ZMQServer {

    private static class Listener implements ZThread.IAttachedRunnable
    {
        @Override
        public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
        {
//              Print everything that arrives on pipe
            while (true) {
                ZFrame frame = ZFrame.recvFrame(pipe);
                if (frame == null)
                    break; //  Interrupted
                frame.print(null);
                frame.destroy();
            }
        }
    }

    public static void main(String[] argv) throws IOException, SQLException {

        Logger logger = LoggerFactory.getLogger(ZMQServer.class);
        logger.info("Starting ZMQServer.");

        int ZMQServerPort = 5555;
        Hashtable<String,String> topicsPlusPortsMap = new Hashtable<>();
        int port = 60000;

        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:" + ZMQServerPort);

            logger.info(String.format("Listening on port %s.", ZMQServerPort));
            logger.info(String.format("Now accepting connection on tcp://localhost:%s.", ZMQServerPort));

            new Thread(() -> {
                try {
                    while (!Thread.interrupted()) {
                        Thread.sleep(10000);
                        Iterator<Map.Entry<String, String>> iterator = topicsPlusPortsMap.entrySet().iterator();
                        logger.info("live publishers listing.");
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> KV = iterator.next();
                            logger.info("[topic:" + KV.getKey() + "] : [port :" + KV.getValue() + "]");
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

                    port++;
                    ZThread.fork(context, new KafkaMessagePublisher(request.getString("kafkaDebeziumServer"), request.getString("topic"), String.valueOf(port)));
                    logger.info(String.format("Publisher for topic '%s' created and live on port %d.", request.getString("topic"), port));
                    topicsPlusPortsMap.put(request.getString("topic"),String.valueOf(port));
                    response = request.put("port",String.valueOf(port)).toString();

                }

                socket.send(response.getBytes(ZMQ.CHARSET), 0);
            }
        }

    }
}
