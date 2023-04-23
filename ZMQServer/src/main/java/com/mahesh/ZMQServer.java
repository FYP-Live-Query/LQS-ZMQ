package com.mahesh;

import com.mahesh.publisher.Publisher;
import com.mahesh.subcriber.Subscriber;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;

import java.io.IOException;
import java.sql.*;

public class ZMQServer {

    private static class Listener implements ZThread.IAttachedRunnable
    {
        @Override
        public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
        {
            //  Print everything that arrives on pipe
//            while (true) {
////                ZFrame frame = ZFrame.recvFrame(pipe);
////                if (frame == null)
////                    break; //  Interrupted
//////                frame.print(null);
////                frame.destroy();
//            }
        }
    }

    public static void main(String[] argv) throws IOException, SQLException {
        try (ZContext ctx = new ZContext()) {
            //  Start child threads
            ZThread.fork(ctx, new Publisher());
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