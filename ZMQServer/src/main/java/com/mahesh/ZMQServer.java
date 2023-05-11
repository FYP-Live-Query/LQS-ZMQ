package com.mahesh;

import com.mahesh.publisher.BinLog.BinLogMessagePublisher;
import com.mahesh.publisher.Kafka.KafkaMessagePublisher;
import com.mahesh.subcriber.Subscriber;
import org.zeromq.*;

import java.io.IOException;
import java.sql.*;

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
        try (ZContext ctx = new ZContext()) {
            //  Start child threads
            ZThread.fork(ctx, new KafkaMessagePublisher("10.8.100.246:9092","dbserver1.inventory.networkTraffic"));
            ZThread.fork(ctx, new Subscriber());

            ZMQ.Socket subscriber = ctx.createSocket(SocketType.XSUB);
            subscriber.connect("tcp://localhost:6000");
            ZMQ.Socket publisher = ctx.createSocket(SocketType.XPUB);
            publisher.bind("tcp://*:6001");
            ZMQ.Socket listener = ZThread.fork(ctx, new Listener());
            ZMQ.proxy(subscriber, publisher, listener);

            System.out.println(" interrupted");
        }
    }
}
