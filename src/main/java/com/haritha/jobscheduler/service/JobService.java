package com.haritha.jobscheduler.service;

import com.haritha.jobscheduler.dto.CreateJobRequest;
import com.haritha.jobscheduler.dto.JobResponse;
import com.haritha.jobscheduler.entity.Job;
import com.haritha.jobscheduler.entity.JobStatus;
import com.haritha.jobscheduler.kafka.KafkaJobProducer;
import com.haritha.jobscheduler.redis.RedisJobStateService;
import com.haritha.jobscheduler.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final RedisJobStateService redisJobStateService;
    private final KafkaJobProducer kafkaJobProducer;

    public JobService(
            JobRepository jobRepository,
            RedisJobStateService redisJobStateService,
            KafkaJobProducer kafkaJobProducer) {

        this.jobRepository = jobRepository;
        this.redisJobStateService = redisJobStateService;
        this.kafkaJobProducer = kafkaJobProducer;
    }

    @Transactional
    public JobResponse createJob(CreateJobRequest request) {

        Job job = new Job();

        job.setName(request.getName());
        job.setType(request.getType());
        job.setPayload(request.getPayload());
        job.setScheduledAt(request.getScheduledAt());

        job.setStatus(JobStatus.SCHEDULED);
        job.setCreatedAt(LocalDateTime.now());
        job.setRetryCount(0);
        job.setMaxRetries(
                request.getMaxRetries() == null
                        ? 3
                        : request.getMaxRetries()
        );

        Job savedJob = jobRepository.save(job);

        redisJobStateService.setJobStatus(
                savedJob.getId(),
                savedJob.getStatus().name()
        );

        // The scheduler publishes it when scheduledAt is reached.
        return convertToResponse(savedJob);
    }

    public List<JobResponse> getAllJobs() {
        return jobRepository.findAll()
                .stream()
                .map(this::convertToResponse)
                .toList();
    }

    public Optional<JobResponse> getJobById(Long id) {
        return jobRepository.findById(id)
                .map(this::convertToResponse);
    }

    public Optional<String> getJobStatus(Long id) {

        if (!jobRepository.existsById(id)) {
            return Optional.empty();
        }

        String redisStatus = redisJobStateService.getJobStatus(id);

        if (redisStatus != null) {
            return Optional.of(redisStatus);
        }

        // Rebuild Redis if the key expired or was temporarily unavailable.
        return jobRepository.findById(id)
                .map(job -> {
                    String status = job.getStatus().name();
                    redisJobStateService.setJobStatus(id, status);
                    return status;
                });
    }


    public java.util.Map<String, Long> getJobStatistics() {

        java.util.Map<String, Long> statistics = new java.util.LinkedHashMap<>();

        for (JobStatus status : JobStatus.values()) {
            long count = jobRepository.findAll().stream()
                    .filter(job -> job.getStatus() == status)
                    .count();
            statistics.put(status.name(), count);
        }

        return statistics;
    }

    @Transactional
    public boolean cancelJob(Long id) {

        int updated = jobRepository.cancelIfCancellable(
                id,
                JobStatus.CANCELLED,
                LocalDateTime.now(),
                "Job cancelled by user",
                List.of(JobStatus.SCHEDULED, JobStatus.QUEUED)
        );

        if (updated == 0) {
            return false;
        }

        redisJobStateService.setJobStatus(
                id,
                JobStatus.CANCELLED.name()
        );

        return true;
    }

    @Transactional
    public void deleteJob(Long id) {

        if (!jobRepository.existsById(id)) {
            return;
        }

        jobRepository.deleteById(id);
        redisJobStateService.deleteJobStatus(id);
    }

    private JobResponse convertToResponse(Job job) {

        JobResponse response = new JobResponse();

        response.setId(job.getId());
        response.setName(job.getName());
        response.setType(job.getType());
        response.setPayload(job.getPayload());
        response.setStatus(job.getStatus());
        response.setScheduledAt(job.getScheduledAt());
        response.setCreatedAt(job.getCreatedAt());
        response.setStartedAt(job.getStartedAt());
        response.setCompletedAt(job.getCompletedAt());
        response.setRetryCount(job.getRetryCount());
        response.setMaxRetries(job.getMaxRetries());
        response.setWorkerId(job.getWorkerId());
        response.setErrorMessage(job.getErrorMessage());

        return response;
    }
}
