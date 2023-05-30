package com.mahesh.publisher.Kafka.KafkaClient.ActiveConsumerRecodHandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.tapestry5.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Consumer;

public class ActiveConsumerRecordHandler<KeyType, ValueType> {
    private final BlockingQueue<ConsumerRecords<KeyType, ValueType>> consumerRecordsList;
    private Consumer<ValueType> consumer;
    private final ExecutorService executorService;
    private final Logger logger = LoggerFactory.getLogger(ActiveConsumerRecordHandler.class);

    public ActiveConsumerRecordHandler(ExecutorService executorService) {
        this.consumerRecordsList = new LinkedBlockingQueue<>();
        this.executorService = executorService;
    }

    public void start() {
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer for message not set");
        }
        Thread thread = new Thread(() -> {
            logger.info("Consumer handler started.");
                while (true) {
                    try {
                        ConsumerRecords<KeyType, ValueType> consumerRecords = consumerRecordsList.take();
                        for (ConsumerRecord<KeyType, ValueType> consumerRecord : consumerRecords) {
                            String strMsg = formatMessage(consumerRecord);
                            logger.info("Sending message to subscribers :" + strMsg);
                            consumer.accept((ValueType) strMsg); // The Java Consumer interface is a functional interface that represents a function that consumes a value without returning any value.
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        );
        thread.start();
        }
    private String formatMessage(ConsumerRecord<KeyType, ValueType> consumerRecord) {
        String stringJsonMsg = consumerRecord.value().toString();
        JSONObject jsonObject = new JSONObject(stringJsonMsg);
        JSONObject extractedMessageContent = (JSONObject) ((JSONObject) jsonObject.get("payload")).get("after");
        jsonObject.clear();
        extractedMessageContent.put("initial_data", "false"); // as required by the backend processing
        JSONObject formattedMessageContent = new JSONObject();
        formattedMessageContent.put("properties", extractedMessageContent); // all user required data for siddhi processing inside properties section in JSON object
        return formattedMessageContent.toString();
    }

    public void setConsumer(Consumer<ValueType> consumer) {
        this.consumer = consumer;
    }

    public void addConsumerRecords(ConsumerRecords<KeyType, ValueType> consumerRecords){
        this.consumerRecordsList.add(consumerRecords);
    }
}
