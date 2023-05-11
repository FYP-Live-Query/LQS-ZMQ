package com.mahesh.publisher.Kafka;

import com.mahesh.publisher.IPublisher;
import com.mahesh.publisher.Kafka.KafkaClient.ActiveConsumerRecodHandling.ActiveConsumerRecordHandler;
import com.mahesh.publisher.Kafka.KafkaClient.AutoOffsetResetConfig;
import com.mahesh.publisher.Kafka.KafkaClient.KafkaConsumerClient;
import io.debezium.common.annotation.Incubating;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

@Incubating
public class KafkaMessagePublisher implements IPublisher
{
    KafkaConsumerClient<String,String> kafkaConsumerClient;

    public KafkaMessagePublisher(String hostName, String topicName){
        this.kafkaConsumerClient = KafkaConsumerClient.<String,String>builder()
                .bootstrap_server_config(hostName) // should we obtain hostname from Config management system?
                .key_deserializer_class_config(StringDeserializer.class)
                .value_deserializer_class_config(StringDeserializer.class)
                .group_id_config("siddhi-io-live-group-" + UUID.randomUUID()) // new subscriber should be in new group for multicasts subscription
                .client_id_config("siddhi-io-live-group-client-" + UUID.randomUUID()) // new subscriber should be in new group for multicasts subscriptio
                .topic(topicName) // should add table name
                .auto_offset_reset_config(AutoOffsetResetConfig.LATEST)
                .activeConsumerRecordHandler(new ActiveConsumerRecordHandler<>())
                .build();
    }
    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {
        ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
        publisher.bind("tcp://*:6000");
        Random rand = new Random(System.currentTimeMillis());
        System.out.println(rand);
        Consumer<String> consumer = o -> {
            String string = String.format("%s:%s","networkTraffic", o);
            try {
                publisher.send(string);
            }catch (Exception e){
                e.printStackTrace();
            }
        };
        kafkaConsumerClient.subscribe();
        kafkaConsumerClient.consumeMessage(consumer);
    }
}
