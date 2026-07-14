package com.taskflow.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskflow.dto.job.JobRequest;
import com.taskflow.dto.job.JobResponse;
import com.taskflow.models.Job;
import com.taskflow.models.enums.JobStatus;
import com.taskflow.models.enums.JobType;
import com.taskflow.repositories.JobRepository;

@ExtendWith(MockitoExtension.class)
public class JobServiceTest {
    @Mock
    private JobRepository jobRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private JobService jobService;

    @Test
    void create_ShouldSaveJobSendToKafkaAndReturnCreatedResponse() throws Exception {
        JobRequest request = new JobRequest(JobType.SEND_EMAIL, Map.of("to", "test@test.com"), 0);
        Job newJob = new Job();
        newJob.setId(UUID.randomUUID());
        newJob.setType(request.type());
        newJob.setStatus(JobStatus.PENDING);

        when(objectMapper.writeValueAsString(request.payload())).thenReturn("{\"to\":\"test@test.com\"}");
        when(jobRepository.save(any(Job.class))).thenReturn(newJob);

        ResponseEntity<JobResponse> response = jobService.create(request);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        verify(jobRepository).save(any(Job.class));
        verify(kafkaTemplate).send(eq("jobs.submitted"), eq(newJob.getId().toString()));
    }

    @Test
    void poll_ShouldReturnOkWithJobResponse() {
        UUID id = UUID.randomUUID();
        Job job = new Job();
        job.setId(id);
        job.setType(JobType.SEND_EMAIL);
        job.setStatus(JobStatus.PENDING);

        when(jobRepository.findById(id)).thenReturn(Optional.of(job));

        ResponseEntity<JobResponse> response = jobService.poll(id);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(id, response.getBody().JobId());
        verify(jobRepository).findById(id);
    }

    @Test
    void poll_ShouldReturnNotFoundWhenJobDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(jobRepository.findById(id)).thenReturn(Optional.empty());

        ResponseEntity<JobResponse> response = jobService.poll(id);
        assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    }
}
