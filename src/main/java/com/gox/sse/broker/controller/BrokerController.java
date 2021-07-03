package com.gox.sse.broker.controller;

import com.gox.sse.broker.dto.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalTime;
import java.util.*;

@RestController
public class BrokerController {

    private static Logger log = LoggerFactory.getLogger(BrokerController.class);

    private Map<String, List<Sinks.Many>> topicSinks = new HashMap<>();
    private Map<String, List<String>> topics = new HashMap<>();

    @GetMapping("/send")
    public void test() {
         if(topicSinks != null){
            this.topicSinks.get("events")
                    .stream()
                    .forEach(sink -> sink.tryEmitNext(new Message("events", LocalTime.now(), "Hello")));
        }
    }

    @Scheduled(fixedRate = 2000)
    public void heartbeat() {
        log.info("heartbeat");
        if(!this.topicSinks.isEmpty()) {
            this.topicSinks.get("heartbeat")
                    .stream()
                    .forEach(sink -> sink.tryEmitNext(new Message("heartbeat", LocalTime.now(), "BEAT")));
        }
    }

    @GetMapping(value = "/topics", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map topics(HttpServletRequest request) {
        return this.topics;
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> events(HttpServletRequest request) {
        String clientId = request.getHeader("clientId");
        List<String> topics = Arrays.asList(request.getHeader("topics").split(","));
        log.info("GET /events : clientId: {}; topics: {}", clientId, topics);

        Sinks.Many sink = Sinks.many().multicast().onBackpressureBuffer();

        for(String topic: topics){
            List<String> clients = this.topics.get(topic);
            if(clients == null){
                clients = new ArrayList<>();
            }
            clients.add(clientId);
            this.topics.put(topic, clients);

            List<Sinks.Many> sinks = this.topicSinks.get(topic);
            if(sinks == null){
                sinks = new ArrayList<>();
                this.topicSinks.put(topic, sinks);
            }
            sinks.add(sink);
        }
        return sink.asFlux();
    }
}

