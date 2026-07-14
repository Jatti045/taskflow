package com.taskflow.controllers;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.taskflow.dto.job.JobRequest;
import com.taskflow.dto.job.JobResponse;
import com.taskflow.services.JobService;

@RestController
@RequestMapping("/jobs")
public class JobController {
    @Autowired
    JobService jobService;

    @PostMapping
    public ResponseEntity<JobResponse> createJob(@RequestBody JobRequest request) throws JsonProcessingException {
        return jobService.create(request);
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> pollJob(@PathVariable UUID id) {
        return jobService.poll(id);
    }
}
