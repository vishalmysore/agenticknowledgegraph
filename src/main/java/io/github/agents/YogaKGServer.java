package io.github.agents;

import io.github.vishalmysore.tools4ai.EnableAgent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@EnableAgent
@PropertySource("classpath:application-yoga.properties")
public class YogaKGServer {
    public static void main(String[] args) {
        SpringApplication.run(YogaKGServer.class, args);
    }
}
