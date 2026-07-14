package com.taskflow.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SendEmailHandler implements JobHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SendEmailHandler.class);

    @Override
    public String handle(String payloadJson) throws Exception {
        LOG.info("Running SendEmailHandler...");

        Thread.sleep(30000);

        return "Email sent successfully from SendEmailHandler";
    }
}
