package com.gox.sse.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SseBrokerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SseBrokerApplication.class, args);
	}

}
