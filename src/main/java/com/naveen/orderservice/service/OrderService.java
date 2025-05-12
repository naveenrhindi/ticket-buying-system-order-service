package com.naveen.orderservice.service;

import com.naveen.bookingservice.event.BookingEvent;
import com.naveen.orderservice.client.InventoryServiceClient;
import com.naveen.orderservice.entity.Order;
import com.naveen.orderservice.repository.OrderRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderService {

    private OrderRepository orderRepository;
    private InventoryServiceClient inventoryServiceClient;

    @Autowired
    public OrderService(OrderRepository orderRepository,
                        InventoryServiceClient inventoryServiceClient) {
        this.orderRepository = orderRepository;
        this.inventoryServiceClient = inventoryServiceClient;
    }

    @KafkaListener(topics = "booking",groupId = "order-service")
    public void orderEvent(BookingEvent bookingEvent){
        log.info("Received order event: {}", bookingEvent);

//        create order object for DB
        Order order = createOrder(bookingEvent);
        orderRepository.saveAndFlush(order);

//        update inventory
        inventoryServiceClient.updateInventory(bookingEvent.getEventId(), bookingEvent.getTicketCount());
        log.info("Inventory updated for event: {}, less tickets: {}", order.getEventId(), order.getTicketCount());

    }

    private Order createOrder(BookingEvent bookingEvent) {
        return Order.builder()
                .customerId(bookingEvent.getUserId())
                .eventId(bookingEvent.getEventId())
                .ticketCount(bookingEvent.getTicketCount())
                .totalPrice(bookingEvent.getTotalPrice())
                .build();
    }
}
