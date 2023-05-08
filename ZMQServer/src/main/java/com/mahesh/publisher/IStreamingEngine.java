package com.mahesh.publisher;

// This interface is to implement strategy design patter for
// differ between kafka,pulsar or any other new streaming engine
// according to user preference
public interface IStreamingEngine<T> {
    void consumeMessage(java.util.function.Consumer<T> consumer); // this method will use by stream thread to consume message (strategy design pattern execute method)
    void subscribe(); // this method will use by stream thread to subscribe to topic (strategy design pattern execute method)
    void unsubscribe(); // this method will use by stream thread to unsubscribe from topic (strategy design pattern execute method)
}
