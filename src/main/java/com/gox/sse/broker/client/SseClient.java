package com.gox.sse.broker.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalTime;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SseClient {

    private static Logger log = LoggerFactory.getLogger(SseClient.class);

    private String brokerEndpoint;
    private HttpClient client;
    private ObjectMapper mapper;

    public SseClient() {
        this.client = HttpClient.newBuilder().build();
        this.mapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
    }

    public void connect(String brokerEndpoint){
        this.brokerEndpoint = brokerEndpoint;
    }

    public Message buildMessageFromJson(String jsonPayload) {
        try {
            return mapper.readValue(jsonPayload.replace("data:", ""), Message.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public void subscribe(String topic, Consumer<Message> consumer){
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(brokerEndpoint + "/" + topic))
                    .build();
            client.send(request, HttpResponse.BodyHandlers.ofLines())
                    .body()
                    .filter(s -> !s.isEmpty())
                    .map(this::buildMessageFromJson)
                    .forEach(consumer);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SseClient sseClient = new SseClient();
        sseClient.connect("http://localhost:8080");

        sseClient.subscribe("heartbeat", message -> {
            System.out.println(message);
        });

    }
}

@Getter
@Setter
@Data
class Message {
    public String topic;
    public LocalTime date;
    public String payload;

}