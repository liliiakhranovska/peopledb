package com.kvitkadev.peopledb.exception;

public class UnableToSaveException extends RuntimeException{
    public UnableToSaveException(String message) {
        super(message);
    }
}
