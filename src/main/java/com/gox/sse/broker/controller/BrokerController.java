package com.gox.sse.broker.controller;

import com.gox.sse.broker.client.SseClient;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalTime;

@RestController
public class BrokerController {

    private static Logger log = LoggerFactory.getLogger(BrokerController.class);

    final Sinks.Many sink;

    public BrokerController() {
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @GetMapping("/send")
    public void test() {
        Sinks.EmitResult result = sink.tryEmitNext(new Msg("kek", LocalTime.now(), "Hello"));
        if (result.isFailure()) {
            // do something here, since emission failed
        }
    }

    @GetMapping(path = "/heartbeat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Msg> streamFlux() {
        log.info("GET /heartbeat");
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> new Msg("heartbeat", LocalTime.now(), "Heartbeat"));
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getResourceUsage() {
        log.info("GET /events");
        return sink.asFlux();
    }
}

@Getter
@Setter
@Data
class Msg {
    public String topic;
    public LocalTime date;
    public String payload;

    public Msg(String heartbeat, LocalTime now, String heartbeat1) {
    }
}