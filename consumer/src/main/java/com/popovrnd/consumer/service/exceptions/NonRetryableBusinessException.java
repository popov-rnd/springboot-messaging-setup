package com.popovrnd.consumer.service.exceptions;

public class NonRetryableBusinessException extends RuntimeException {
    public NonRetryableBusinessException(String message) {
        super(message);
    }
}
