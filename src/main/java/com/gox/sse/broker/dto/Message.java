package com.gox.sse.broker.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalTime;
import java.util.UUID;

@Getter
@Setter
@ToString
public class Message {
    public UUID clientId;
    public String topic;
    public LocalTime date;
    public String payload;

    public Message(String topic, LocalTime date, String payload) {
        this.topic = topic;
        this.date = date;
        this.payload = payload;
    }
}