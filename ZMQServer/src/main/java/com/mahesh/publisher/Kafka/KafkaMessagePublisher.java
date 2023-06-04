package com.mahesh.publisher.Kafka;

import com.mahesh.publisher.IPublisher;
import com.mahesh.publisher.Kafka.KafkaClient.ActiveConsumerRecodHandling.ActiveConsumerRecordHandler;
import com.mahesh.publisher.Kafka.KafkaClient.Config.AutoOffsetResetConfig;
import com.mahesh.publisher.Kafka.KafkaClient.KafkaConsumerClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.tapestry5.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class KafkaMessagePublisher implements IPublisher
{
    private final Logger logger = LoggerFactory.getLogger(KafkaMessagePublisher.class);
    private final KafkaConsumerClient<String,String> KAFKA_CONSUMER_CLIENT;
    private final String PORT;
    private final String TOPIC_NAME;
    private static final AtomicInteger publishers = new AtomicInteger(0);
    public KafkaMessagePublisher(final String hostName, final String kafkaTopic, final String port){

        publishers.incrementAndGet();
        this.PORT = port;
        ExecutorService executorService = new ThreadPoolExecutor(
                                                        Runtime.getRuntime().availableProcessors(),
                                                        Runtime.getRuntime().availableProcessors(),
                                                        100,
                                                        TimeUnit.SECONDS,
                                                        new ArrayBlockingQueue<>(10));
        this.TOPIC_NAME = kafkaTopic;
        ActiveConsumerRecordHandler<String,String> activeConsumerRecordHandler =
                new ActiveConsumerRecordHandler<>(executorService);
        this.KAFKA_CONSUMER_CLIENT = KafkaConsumerClient.<String,String>builder()
                .bootstrap_server_config(hostName) // should we obtain hostname from Config management system?
                .key_deserializer_class_config(StringDeserializer.class)
                .value_deserializer_class_config(StringDeserializer.class)
                .group_id_config("zmq-pub-group-" + UUID.randomUUID()) // new subscriber should be in new group for multicasts subscription
                .client_id_config("zmq-pub-group-client-" + UUID.randomUUID()) // new subscriber should be in new group for multicasts subscriptio
                .topic(kafkaTopic) // should add table name
                .auto_offset_reset_config(AutoOffsetResetConfig.LATEST)
                .activeConsumerRecordHandler(activeConsumerRecordHandler)
                .build();
    }
    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {
        ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(String.format("Unbinding publisher \"tcp://*:%s\" " ,PORT));
            publisher.unbind("tcp://*:" + PORT);
        }));

        publisher.bind("tcp://*:" + this.PORT);

        Consumer<String> consumer = (message) -> {
            String stringMessage = String.format("%s:%s",this.TOPIC_NAME, message);
            publisher.send(stringMessage);
        };

        KAFKA_CONSUMER_CLIENT.subscribe();
        KAFKA_CONSUMER_CLIENT.consumeMessage(consumer);
    }


    public static int getNumOfPublishers() {
        return publishers.get();
    }
}
