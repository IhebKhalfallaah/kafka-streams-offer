package org.example.backendsse.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class SseKafkaBridge {

    private static final Logger log = LoggerFactory.getLogger(SseKafkaBridge.class);

    private final List<SseEmitter> acceptedEmitters = new CopyOnWriteArrayList<>();
    private final List<SseEmitter> rejectedEmitters = new CopyOnWriteArrayList<>();
    private final List<SseEmitter> ordersEmitters   = new CopyOnWriteArrayList<>();

    // Add connection status tracking
    private volatile boolean kafkaConnected = true;

    public SseEmitter addAcceptedEmitter() {
        return registerEmitter(acceptedEmitters);
    }

    public SseEmitter addRejectedEmitter() {
        return registerEmitter(rejectedEmitters);
    }

    public SseEmitter addOrdersEmitter() {
        return registerEmitter(ordersEmitters);
    }

    private SseEmitter registerEmitter(List<SseEmitter> bucket) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        bucket.add(emitter);

        emitter.onCompletion(() -> {
            bucket.remove(emitter);
            log.debug("Emitter completed, remaining: {}", bucket.size());
        });

        emitter.onTimeout(() -> {
            emitter.complete();
            bucket.remove(emitter);
            log.debug("Emitter timeout, remaining: {}", bucket.size());
        });

        emitter.onError(e -> {
            log.warn("SSE emitter error: {}", e.getMessage());
            bucket.remove(emitter);
            log.debug("Emitter error, remaining: {}", bucket.size());
        });

        try {
            // Send connection status on init
            String status = kafkaConnected ? "connected" : "kafka_disconnected";
            emitter.send(SseEmitter.event().name("init").data(status));
        } catch (IOException e) {
            bucket.remove(emitter);
            emitter.complete();
        }

        return emitter;
    }

    private void broadcast(List<SseEmitter> bucket, String eventName, String jsonPayload) {
        if (bucket.isEmpty()) {
            return;
        }

        List<SseEmitter> deadEmitters = new ArrayList<>();

        for (SseEmitter emitter : bucket) {
            try {
                emitter.send(
                        SseEmitter.event()
                                .name(eventName)
                                .data(jsonPayload)
                );
            } catch (Exception e) {
                log.debug("Removing dead SSE emitter: {}", e.getMessage());
                deadEmitters.add(emitter);
            }
        }

        // Remove dead emitters after iteration to avoid ConcurrentModificationException
        bucket.removeAll(deadEmitters);
        for (SseEmitter dead : deadEmitters) {
            dead.complete();
        }
    }

    // ===== Kafka listeners with error handling =====

    @KafkaListener(
            topics = "offer.accepted",
            groupId = "ui-accepted-listener"
    )
    public void onAccepted(ConsumerRecord<String, String> record) {
        log.debug("Received accepted offer: {}", record.value());
        String value = record.value();
        broadcast(acceptedEmitters, "accepted", value);
    }

    @KafkaListener(
            topics = "offer.rejected",
            groupId = "ui-rejected-listener"
    )
    public void onRejected(ConsumerRecord<String, String> record) {
        log.debug("Received rejected offer: {}", record.value());
        String value = record.value();
        broadcast(rejectedEmitters, "rejected", value);
    }

    @KafkaListener(
            topics = "order.created",
            groupId = "ui-orders-listener"
    )
    public void onOrder(ConsumerRecord<String, String> record) {
        log.debug("Received order: {}", record.value());
        String value = record.value();
        broadcast(ordersEmitters, "order", value);
    }

    // Listen for Kafka connection events
    @EventListener
    public void handleKafkaEvent(ListenerContainerIdleEvent event) {
        log.debug("Kafka listener idle: {}", event);
    }
}