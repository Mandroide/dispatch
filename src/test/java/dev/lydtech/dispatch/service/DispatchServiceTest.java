package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import dev.lydtech.message.DispatchPreparing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class DispatchServiceTest {
    @InjectMocks
    private DispatchService dispatchService;
    @Mock
    private KafkaTemplate<String, Object> kafkaProducer;
    @Mock
    private CompletableFuture<SendResult<String, Object>> completableFuture;

    @Test
    void process_Success() throws ExecutionException, InterruptedException {
        Mockito.when(kafkaProducer.send(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(DispatchPreparing.class)))
                .thenReturn(completableFuture);
        Mockito.when(kafkaProducer.send(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(OrderDispatched.class)))
                .thenReturn(completableFuture);
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), UUID.randomUUID().toString());
        dispatchService.process(key, orderCreated);
        Mockito.verify(kafkaProducer, Mockito.times(1)).send(Mockito.eq("dispatch.tracking"),
                Mockito.eq(key),
                Mockito.any(DispatchPreparing.class));
        Mockito.verify(kafkaProducer, Mockito.times(1)).send(Mockito.eq("order.dispatched"),
                Mockito.eq(key),
                Mockito.any(OrderDispatched.class));
    }

    @Test
    void process_DispatchTrackingProducerThrowsException() {
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), UUID.randomUUID().toString());
        Mockito.doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducer).send(Mockito.eq("dispatch.tracking"),
                Mockito.anyString(),
                ArgumentMatchers.any(DispatchPreparing.class));

        RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, () -> dispatchService.process(key, orderCreated));

        Mockito.verify(kafkaProducer, Mockito.times(1)).send(Mockito.eq("dispatch.tracking"),
                Mockito.eq(key),
                Mockito.any(DispatchPreparing.class));
        Mockito.verifyNoMoreInteractions(kafkaProducer);

        assertThat(runtimeException.getMessage()).isEqualTo("dispatch tracking producer failure");
    }

    @Test
    void process_OrderDispatchedProducerThrowsException() {
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), UUID.randomUUID().toString());
        Mockito.when(kafkaProducer.send(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any(DispatchPreparing.class)))
                .thenReturn(completableFuture);
        Mockito.doThrow(new RuntimeException("order dispatched producer failed", null)).when(kafkaProducer).send(Mockito.eq("order.dispatched"),
                Mockito.anyString(),
                ArgumentMatchers.any(OrderDispatched.class));

        RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, () -> dispatchService.process(key, orderCreated));

        Mockito.verify(kafkaProducer, Mockito.times(1)).send(Mockito.eq("dispatch.tracking"),
                Mockito.eq(key),
                Mockito.any(DispatchPreparing.class));
        Mockito.verify(kafkaProducer, Mockito.times(1)).send(Mockito.eq("order.dispatched"),
                Mockito.eq(key),
                Mockito.any(OrderDispatched.class));

        assertThat(runtimeException.getMessage()).isEqualTo("order dispatched producer failed");
    }
}
