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

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;

@RestController
public class BrokerController {

    private static Logger log = LoggerFactory.getLogger(BrokerController.class);

    final Sinks.Many sink;

    private HashMap<String, Sinks.Many> topicSinks;

    public BrokerController() {
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @GetMapping("/send")
    public void test() {
        Sinks.EmitResult result = sink.tryEmitNext(new Message("events", LocalTime.now(), "Hello"));
    }

    @Scheduled(fixedRate = 2000)
    public void heartbeat() {
        log.info("heartbeat");
        sink.tryEmitNext(new Message("heartbeat", LocalTime.now(), "Heartbeat"));
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getResourceUsage() {
        log.info("GET /events");
        return sink.asFlux();
    }
}

