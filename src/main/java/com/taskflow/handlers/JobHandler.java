package com.taskflow.handlers;

public interface JobHandler {
    String handle(String payloadJson) throws Exception;
}
