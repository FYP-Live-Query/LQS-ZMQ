package com.mahesh.publisher;

import io.debezium.common.annotation.Incubating;
import org.zeromq.ZThread;
@Incubating
public interface IPublisher extends ZThread.IAttachedRunnable{
}
