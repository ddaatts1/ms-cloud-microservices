package com.micro.product_service.Reposistory;


import com.micro.product_service.Entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}