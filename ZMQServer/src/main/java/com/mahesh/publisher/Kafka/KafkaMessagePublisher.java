package com.mahesh.publisher.Kafka;

import com.mahesh.publisher.IPublisher;
import io.debezium.common.annotation.Incubating;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
@Incubating
public class KafkaMessagePublisher implements IPublisher
{
    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {

    }
}
