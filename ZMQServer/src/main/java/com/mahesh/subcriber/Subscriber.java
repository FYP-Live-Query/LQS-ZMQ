package com.mahesh.subcriber;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;

public class Subscriber implements ZThread.IAttachedRunnable
{
    @Override
    public void run(Object[] args, ZContext ctx, ZMQ.Socket pipe)
    {
        //  Subscribe to "A" and "B"
        ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.connect("tcp://localhost:6000");
        subscriber.subscribe("networkTraffic".getBytes(ZMQ.CHARSET));

        int count = 0;
        while (true) {
            String string = subscriber.recvStr();
            System.out.println(string);
            if (string == null)
                break; //  Interrupted
            count++;
        }
        ctx.destroySocket(subscriber);
    }
}
