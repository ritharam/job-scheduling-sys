package com.haritha.jobscheduler.controller;

import com.haritha.jobscheduler.dto.CreateJobRequest;
import com.haritha.jobscheduler.dto.JobResponse;
import com.haritha.jobscheduler.service.JobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> createJob(
            @Valid @RequestBody CreateJobRequest request) {

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(jobService.createJob(request));
    }

    @GetMapping
    public ResponseEntity<List<JobResponse>> getAllJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJobById(
            @PathVariable Long id) {

        return jobService.getJobById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Long>> getJobStatistics() {
        return ResponseEntity.ok(jobService.getJobStatistics());
    }

    @GetMapping("/{id}/status")
    public ResponseEntity<Map<String, Object>> getJobStatus(
            @PathVariable Long id) {

        return jobService.getJobStatus(id)
                .map(status -> {

                    Map<String, Object> response = new HashMap<>();
                    response.put("jobId", id);
                    response.put("status", status);

                    return ResponseEntity.ok(response);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJob(
            @PathVariable Long id) {

        if (!jobService.getJobById(id).isPresent()) {
            return ResponseEntity.notFound().build();
        }

        boolean cancelled = jobService.cancelJob(id);

        if (!cancelled) {

            Map<String, Object> response = new HashMap<>();
            response.put("jobId", id);
            response.put(
                    "message",
                    "Job cannot be cancelled because it is already RUNNING, SUCCESS, FAILED, or CANCELLED."
            );

            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(response);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("jobId", id);
        response.put("status", "CANCELLED");

        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteJob(
            @PathVariable Long id) {

        if (!jobService.getJobById(id).isPresent()) {
            return ResponseEntity.notFound().build();
        }

        jobService.deleteJob(id);

        return ResponseEntity.noContent().build();
    }
}
