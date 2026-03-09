package com.micro.order_service.Controller;

import com.micro.order_service.Entity.Order;
import com.micro.order_service.Reposistory.OrderRepository;
import com.micro.order_service.dto.OrderResponse;
import com.micro.order_service.dto.ProductResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {

    private final OrderRepository repository;
    private final RestTemplate restTemplate;

    @Value("${product.service.url}")
    private String productServiceUrl;

    @GetMapping("/hello")
    public String hello() {
        return "Hello from Order Service";
    }

    @GetMapping({"", "/"})
    public List<OrderResponse> getAll() {

        return repository.findAll()
                .stream()
                .map(order -> {

                    ProductResponse product =
                            restTemplate.getForObject(
                                    productServiceUrl + "/products/" + order.getProductId(),
                                    ProductResponse.class
                            );

                    return OrderResponse.builder()
                            .id(order.getId())
                            .quantity(order.getQuantity())
                            .product(product)
                            .build();
                })
                .collect(Collectors.toList());
    }
}