package com.haritha.jobscheduler.worker;

import com.haritha.jobscheduler.entity.Job;
import com.haritha.jobscheduler.entity.JobStatus;
import com.haritha.jobscheduler.redis.RedisJobStateService;
import com.haritha.jobscheduler.repository.JobRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class JobWorker {

    private static final String TOPIC = "job-execution";

    private final JobRepository jobRepository;
    private final RedisJobStateService redisJobStateService;
    private final String workerId;

    public JobWorker(
            JobRepository jobRepository,
            RedisJobStateService redisJobStateService,
            @Value("${job.worker.id:worker-${random.uuid}}") String workerId) {

        this.jobRepository = jobRepository;
        this.redisJobStateService = redisJobStateService;
        this.workerId = workerId;
    }

    @KafkaListener(
            topics = TOPIC,
            groupId = "${spring.kafka.consumer.group-id:job-worker-group}"
    )
    public void processJob(String message) {

        final Long jobId;

        try {
            jobId = Long.parseLong(message.trim());
        } catch (NumberFormatException e) {
            System.err.println("Ignoring invalid Kafka message: " + message);
            return;
        }

        System.out.println("========================================");
        System.out.println("Worker received job: " + jobId);
        System.out.println("Worker ID: " + workerId);
        System.out.println("========================================");

        // Atomic DB claim prevents duplicate Kafka messages/workers
        // from executing the same queued job.
        if (!claimJob(jobId)) {

            System.out.println(
                    "Job " + jobId +
                    " was already claimed, cancelled, or is not QUEUED."
            );

            return;
        }

        Job job = jobRepository.findById(jobId)
                .orElse(null);

        if (job == null) {
            System.out.println("Job not found after claim: " + jobId);
            return;
        }

        redisJobStateService.setJobStatus(
                jobId,
                JobStatus.RUNNING.name()
        );

        System.out.println("Job is now RUNNING: " + jobId);
        System.out.println("Executing job: " + jobId);

        try {

            // Simulate actual job execution.
            Thread.sleep(3000);

            // Test failure by putting "FAIL" in the payload.
            if (job.getPayload() != null &&
                    job.getPayload().toUpperCase().contains("FAIL")) {

                throw new RuntimeException(
                        "Simulated job execution failure"
                );
            }

            completeSuccessfully(jobId);

            System.out.println(
                    "Job completed successfully: " + jobId
            );

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            handleFailure(
                    jobId,
                    "Worker thread was interrupted"
            );

        } catch (Exception e) {

            handleFailure(
                    jobId,
                    e.getMessage() == null
                            ? "Unknown job execution error"
                            : e.getMessage()
            );
        }
    }

    @Transactional
    protected boolean claimJob(Long jobId) {

        int updated = jobRepository.claimQueuedJob(
                jobId,
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                LocalDateTime.now(),
                workerId
        );

        return updated == 1;
    }

    @Transactional
    protected void completeSuccessfully(Long jobId) {

        Job job = jobRepository.findById(jobId)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Job not found: " + jobId
                        ));

        job.setStatus(JobStatus.SUCCESS);
        job.setCompletedAt(LocalDateTime.now());
        job.setErrorMessage(null);

        jobRepository.save(job);

        redisJobStateService.setJobStatus(
                jobId,
                JobStatus.SUCCESS.name()
        );
    }

    @Transactional
    protected void handleFailure(Long jobId, String errorMessage) {

        Job job = jobRepository.findById(jobId)
                .orElse(null);

        if (job == null) {
            return;
        }

        int retryCount =
                job.getRetryCount() == null
                        ? 0
                        : job.getRetryCount();

        int maxRetries =
                job.getMaxRetries() == null
                        ? 0
                        : job.getMaxRetries();

        retryCount++;

        job.setRetryCount(retryCount);
        job.setErrorMessage(errorMessage);

        if (retryCount <= maxRetries) {

            // Retry after a small delay through the scheduler.
            job.setStatus(JobStatus.SCHEDULED);
            job.setScheduledAt(
                    LocalDateTime.now().plusSeconds(5)
            );
            job.setCompletedAt(null);

            jobRepository.save(job);

            redisJobStateService.setJobStatus(
                    jobId,
                    JobStatus.SCHEDULED.name()
            );

            System.out.println(
                    "Retry scheduled for job " +
                    jobId +
                    " (" +
                    retryCount +
                    "/" +
                    maxRetries +
                    ")"
            );

        } else {

            job.setStatus(JobStatus.FAILED);
            job.setCompletedAt(LocalDateTime.now());

            jobRepository.save(job);

            redisJobStateService.setJobStatus(
                    jobId,
                    JobStatus.FAILED.name()
            );

            System.out.println(
                    "Job permanently failed: " + jobId
            );

            System.out.println(
                    "Retry limit reached: " +
                    retryCount +
                    "/" +
                    maxRetries
            );
        }
    }
}
