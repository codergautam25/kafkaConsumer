package com.example.kafka_consumer.controller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private final String NEW_TOPIC = "forwarded_data";  // Topic where the message will be forwarded

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // This method listens to the producer_test topic, and once it consumes a message, it will forward it to the new_topic
    @KafkaListener(topics = "producer_test", groupId = "group1")
    public void consumeAndForwardMessage(String message) {
        System.out.println("Consumed message from producer_test: " + message);
        
        // Forward the message to a new topic
        kafkaTemplate.send(NEW_TOPIC, message);
        System.out.println("Message forwarded to new_topic: " + message);
    }

    // Create an API endpoint to manually trigger the consume and forward process (in case you want to manually test the flow)
    @GetMapping("/consumeAndForward")
    public String consumeAndForwardMessageAPI() {
        // This is just an example to invoke manually from an API if you want to test
        // You can use a more robust approach based on message-processing logic.
        kafkaTemplate.send(NEW_TOPIC, "Sample message for new topic");
        return "Message manually forwarded to " + NEW_TOPIC;
    }
}
