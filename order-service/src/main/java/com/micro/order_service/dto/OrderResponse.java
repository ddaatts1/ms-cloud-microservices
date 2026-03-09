package com.micro.order_service.dto;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderResponse {
    private Long id;
    private Integer quantity;
    private ProductResponse product;
}