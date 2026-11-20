package org.example.backendsse.controller;


import org.example.backendsse.service.SseKafkaBridge;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@CrossOrigin(origins = "*") // allow React on localhost:5173/3000/whatever
public class SseController {

    private final SseKafkaBridge bridge;

    public SseController(SseKafkaBridge bridge) {
        this.bridge = bridge;
    }

    @GetMapping("/sse/accepted")
    public SseEmitter acceptedStream() {
        return bridge.addAcceptedEmitter();
    }

    @GetMapping("/sse/rejected")
    public SseEmitter rejectedStream() {
        return bridge.addRejectedEmitter();
    }

    @GetMapping("/sse/orders")
    public SseEmitter ordersStream() {
        return bridge.addOrdersEmitter();
    }
}
