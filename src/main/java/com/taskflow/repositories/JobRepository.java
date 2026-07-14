package com.taskflow.repositories;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.taskflow.models.Job;
import com.taskflow.models.enums.JobStatus;

public interface JobRepository extends JpaRepository<Job, UUID> {
    List<Job> findByJobStatus(JobStatus status);
}
