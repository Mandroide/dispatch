package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {
    private final DispatchService dispatchService;

    @KafkaListener(id = "orderConsumerClient", topics = "order.created",
            groupId = "dispatch.order.created.consumer2", containerFactory = "kafkaListenerContainerFactoryObj")
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Payload OrderCreated payload) {
        log.info("Received message: partition: {} - key: {} - payload: {}", partition, key, payload);
        try {
            dispatchService.process(key, payload);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processing failure" , e);
        }
    }
}
