package com.taskflow.services;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import com.taskflow.dto.job.JobRequest;
import com.taskflow.dto.job.JobResponse;
import com.taskflow.models.Job;
import com.taskflow.models.enums.JobStatus;
import com.taskflow.repositories.JobRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class JobService {
    private static final Logger LOG = LoggerFactory.getLogger(JobService.class);
    private static final String TOPIC = "jobs.submitted";

    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private ThreadPoolTaskScheduler taskScheduler;

    public ResponseEntity<JobResponse> create(JobRequest request) throws JsonProcessingException {
        Job job = new Job();

        job.setType(request.type());
        job.setPayload(objectMapper.writeValueAsString(request.payload()));
        job.setStatus(JobStatus.PENDING);

        Job createdJob = jobRepository.save(job);
        runJob(createdJob.getId(), Objects.requireNonNullElse(request.delayInSeconds(), 0));

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(responseSummary(createdJob));
    }

    public ResponseEntity<JobResponse> poll(UUID id) {
        Optional<Job> jobOptional = jobRepository.findById(id);
        if (!jobOptional.isPresent())
            return ResponseEntity.notFound().build();

        Job job = jobOptional.get();
        return ResponseEntity
                .ok(responseSummary(job));
    }

    private void runJob(UUID jobId, int delayInSeconds) {
        Runnable task = () -> {
            kafkaTemplate.send(TOPIC, jobId.toString());
        };

        taskScheduler.schedule(task, Instant.now().plusSeconds(delayInSeconds));
    }

    private JobResponse responseSummary(Job job) {
        return new JobResponse(job.getId(), job.getType(), job.getStatus(),
                job.getResult(), job.getError(), job.getCreated_at(),
                job.getStarted_at(),
                job.getFinished_at());
    }
}
