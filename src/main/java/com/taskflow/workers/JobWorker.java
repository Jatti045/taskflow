package com.taskflow.workers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.taskflow.handlers.JobHandler;
import com.taskflow.handlers.SendEmailHandler;
import com.taskflow.models.Job;
import com.taskflow.models.enums.JobStatus;
import com.taskflow.models.enums.JobType;
import com.taskflow.repositories.JobRepository;

@Component
public class JobWorker {
    private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);
    private static final int MAX_RETRY_COUNT = 3;
    private static final int BASE_DELAY = 10_000;
    private static final String JOBS_SUBMITTED_TOPIC = "jobs.submitted";
    private static final String JOBS_DEAD_TOPIC = "jobs.dead";
    private static final String GROUP = "taskflow-workers";
    private final Map<JobType, JobHandler> handlers;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    public JobWorker(SendEmailHandler sendEmailHandler) {
        this.handlers = Map.of(JobType.SEND_EMAIL, sendEmailHandler);
    }

    @KafkaListener(topics = JOBS_SUBMITTED_TOPIC, groupId = GROUP)
    public void consume(String jobId) throws InterruptedException {
        Job job = jobRepository.findById(UUID.fromString(jobId)).orElseThrow(RuntimeException::new);

        job.setStarted_at(Instant.now());
        job.setStatus(JobStatus.RUNNING);
        jobRepository.save(job);

        try {
            String result = handlers.get(job.getType()).handle(job.getPayload());
            job.setStatus(JobStatus.SUCCEEDED);
            job.setResult(result);
            job.setFinished_at(Instant.now());
        } catch (Exception e) {
            LOG.error(e.getMessage());
            job.setError(e.getMessage());
            retryJob(job);
        } finally {
            jobRepository.save(job);
        }
    }

    private void retryJob(Job job) throws InterruptedException {
        if (job.getRetryCount() < MAX_RETRY_COUNT) {
            job.setRetryCount(job.getRetryCount() + 1);
            job.setStatus(JobStatus.PENDING);
            job.setFinished_at(Instant.now());
            Thread.sleep((long) (BASE_DELAY * Math.pow(2, job.getRetryCount())));
            kafkaTemplate.send(JOBS_SUBMITTED_TOPIC, job.getId().toString());
        } else {
            job.setStatus(JobStatus.FAILED);
            job.setFinished_at(Instant.now());
            kafkaTemplate.send(JOBS_DEAD_TOPIC, job.getId().toString());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recoverStuckJobs() {
        List<Job> stuckJobs = jobRepository.findByJobStatus(JobStatus.RUNNING);
        for (Job job : stuckJobs) {
            job.setStatus(JobStatus.PENDING);
            jobRepository.save(job);
            kafkaTemplate.send(JOBS_SUBMITTED_TOPIC, job.getId().toString());
        }
    }
}
