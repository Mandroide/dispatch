package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.service.DispatchService;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

class OrderCreatedHandlerTest {
    private OrderCreatedHandler handler;
    private DispatchService dispatchService;

    @BeforeEach
    void setUp() {
        dispatchService = Mockito.mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchService);
    }

    @Test
    void listen_Success() throws ExecutionException, InterruptedException {
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), UUID.randomUUID().toString());
        handler.listen(0, key, orderCreated);
        Mockito.verify(dispatchService, Mockito.times(1)).process(key, orderCreated);
    }

    @Test
    void listen_ServiceThrowsException() throws ExecutionException, InterruptedException {
        String key = UUID.randomUUID().toString();
        OrderCreated orderCreated = TestEventData.createOrder(UUID.randomUUID(), UUID.randomUUID().toString());
        Mockito.doThrow(new ExecutionException("Service fail", null)).when(dispatchService).process(key, orderCreated);
        handler.listen(0, key, orderCreated);
        Mockito.verify(dispatchService, Mockito.times(1)).process(key, orderCreated);
    }
}