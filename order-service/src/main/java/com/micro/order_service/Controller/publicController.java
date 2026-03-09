package com.micro.order_service.Controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class publicController {


    @GetMapping
    public String index(){
        return "hello";
    }
}
