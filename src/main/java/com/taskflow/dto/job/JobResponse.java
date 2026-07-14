package com.taskflow.dto.job;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.taskflow.models.enums.JobStatus;
import com.taskflow.models.enums.JobType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobResponse(
                UUID JobId,
                JobType type,
                JobStatus status,
                String result,
                String error,
                Instant createdAt,
                Instant startedAt,
                Instant finishedAt) {
}
