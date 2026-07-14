package com.taskflow.dto.job;

import java.util.Map;

import com.taskflow.models.enums.JobType;

public record JobRequest(
                JobType type,
                Map<String, Object> payload,
                Integer delayInSeconds) {
}
