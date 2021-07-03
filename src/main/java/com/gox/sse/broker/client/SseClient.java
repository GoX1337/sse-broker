package com.gox.sse.broker.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.gox.sse.broker.dto.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.function.Consumer;

public class SseClient {

    private static Logger log = LoggerFactory.getLogger(SseClient.class);

    private HttpClient client;
    private ObjectMapper mapper;
    private String brokerEndpoint;
    private Map<String, Consumer<Message>> messageHandlers;
    private List<String> topics;
    private UUID id = UUID.randomUUID();

    public SseClient(String brokerEndpoint, Map<String, Consumer<Message>> messageHandlers) {
        this.brokerEndpoint = brokerEndpoint;
        this.messageHandlers = messageHandlers;
        this.client = HttpClient.newBuilder().build();
        this.mapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
    }

    public static class Builder {

        private String brokerEndpoint;
        private Map<String, Consumer<Message>> messageHandlers = new HashMap<>();
        private List<String> topics = new ArrayList<>();

        public Builder endpoint(String brokerEndpoint){
            this.brokerEndpoint = brokerEndpoint;
            return this;
        }

        public Builder messageHandler(String topic, Consumer<Message> handler){
            topics.add(topic);
            messageHandlers.put(topic, handler);
            return this;
        }

        public SseClient build(){
            return new SseClient(this.brokerEndpoint, this.messageHandlers);
        }
    }

    public Message buildMessageFromJson(String jsonPayload) {
        try {
            Message msg = mapper.readValue(jsonPayload.replace("data:", ""), Message.class);
            msg.setClientId(this.id);
            return msg;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private void start() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(brokerEndpoint + "/events"))
                    .build();

            client.send(request, HttpResponse.BodyHandlers.ofLines())
                    .body()
                    .filter(s -> !s.isEmpty())
                    .map(this::buildMessageFromJson)
                    .forEach(message -> {
                        Consumer<Message> messageHandler = this.messageHandlers.get(message.getTopic());
                        if(messageHandler != null){
                            messageHandler.accept(message);
                        } else {
                            log.error("No message handler for topic {}", message.getTopic());
                        }
                    });
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Thread(() -> {
            SseClient sseClient = new SseClient.Builder()
                    .endpoint("http://localhost:8080")
                    .messageHandler("heartbeat", message -> {
                        System.out.println("/heartbeat : " + message);
                    })
                    .messageHandler("events", message -> {
                        System.out.println("/event : " + message);
                    })
                    .build();
            sseClient.start();
        }).start();

        SseClient sseClient = new SseClient.Builder()
                .endpoint("http://localhost:8080")
                .messageHandler("heartbeat", message -> {
                    System.out.println("/heartbeat : " + message);
                })
                .build();
        sseClient.start();
    }
}