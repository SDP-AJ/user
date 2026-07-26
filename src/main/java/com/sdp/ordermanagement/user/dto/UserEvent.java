package com.sdp.ordermanagement.user.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;
    private String eventType;

    private UserDetails userDetails;

    private List<OrderItem> items;

    private Map<String, String> metadata;

    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdAt;


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class UserDetails implements Serializable {

        private static final long serialVersionUID = 1L;

        private String userId;
        private String name;
        private String email;
        private Address address;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Address implements Serializable {

        private static final long serialVersionUID = 1L;

        private String street;
        private String city;
        private String state;
        private String zipCode;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OrderItem implements Serializable {

        private static final long serialVersionUID = 1L;

        private String productId;
        private String productName;
        private int quantity;
        private double price;
    }
}