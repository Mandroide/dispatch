package dev.lydtech.dispatch.integration;

import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import dev.lydtech.message.DispatchCompleted;
import dev.lydtech.message.DispatchPreparing;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = DispatchConfiguration.class)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, kraft = true, bootstrapServersProperty = "kafka.bootstrap-servers")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderDispatchedIntegrationTest {
    private static final String ORDER_CREATED_TOPIC = "order.created";
    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private DispatchTrackingListener dispatchTrackingListener;
    @Autowired
    private OrderDispatchedListener orderDispatchedListener;
    @Autowired
    private EmbeddedKafkaKraftBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Component
    @KafkaListener(groupId = "KafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
    static class DispatchTrackingListener {
        AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
        AtomicInteger dispatchCompletedCounter = new AtomicInteger(0);

        @KafkaHandler
        void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                      @Payload DispatchPreparing payload) {
            log.debug("Received DispatchPreparing key: {} - payload: {}", key, payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            dispatchPreparingCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveDispatchCompleted(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                      @Payload DispatchCompleted payload) {
            log.debug("Received receiveDispatchCompleted key: {} - payload: {}", key, payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            dispatchCompletedCounter.incrementAndGet();
        }
    }

    @Component
    @KafkaListener(groupId = "KafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
    static class OrderDispatchedListener {
        AtomicInteger orderDispatchedCounter = new AtomicInteger(0);

        @KafkaHandler
        void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                                    @Payload OrderDispatched payload) {
            log.debug("Received OrderDispatched key: {} - payload: {}", key, payload);
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            orderDispatchedCounter.incrementAndGet();
        }
    }

    @BeforeEach
    void setUp() {
        orderDispatchedListener.orderDispatchedCounter.set(0);
        dispatchTrackingListener.dispatchPreparingCounter.set(0);
        dispatchTrackingListener.dispatchCompletedCounter.set(0);

        // Wait until the partitions are assigned. The application listener container has one topic and the test
        // listener container has multiple topics, so take that into account when awaiting for topic assignment.
//        registry.getListenerContainers().forEach(container ->
//                ContainerTestUtils.waitForAssignment(container,
//                        Objects.requireNonNull(container.getContainerProperties().getTopics()).length
//                                * embeddedKafkaBroker.getPartitionsPerTopic()));

        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @Test
    void testOrderDispatchFlow() throws ExecutionException, InterruptedException {
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), "my item");
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);

        await().atMost(3L, TimeUnit.SECONDS).pollDelay(100L, TimeUnit.MILLISECONDS)
                .untilAtomic(orderDispatchedListener.orderDispatchedCounter, equalTo(1));
        await().atMost(1L, TimeUnit.SECONDS).pollDelay(100L, TimeUnit.MILLISECONDS)
                .untilAtomic(dispatchTrackingListener.dispatchPreparingCounter, equalTo(1));
        await().atMost(1L, TimeUnit.SECONDS).pollDelay(100L, TimeUnit.MILLISECONDS)
                .untilAtomic(dispatchTrackingListener.dispatchCompletedCounter, equalTo(1));

    }

    private void sendMessage(String topic, String key, Object orderCreated) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(MessageBuilder.withPayload(orderCreated)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)

                .build()).get();
    }
}
