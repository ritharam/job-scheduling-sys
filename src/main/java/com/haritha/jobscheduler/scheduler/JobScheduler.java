package com.haritha.jobscheduler.scheduler;

import com.haritha.jobscheduler.entity.Job;
import com.haritha.jobscheduler.entity.JobStatus;
import com.haritha.jobscheduler.kafka.KafkaJobProducer;
import com.haritha.jobscheduler.redis.RedisJobStateService;
import com.haritha.jobscheduler.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class JobScheduler {

    private final JobRepository jobRepository;
    private final KafkaJobProducer kafkaJobProducer;
    private final RedisJobStateService redisJobStateService;

    public JobScheduler(
            JobRepository jobRepository,
            KafkaJobProducer kafkaJobProducer,
            RedisJobStateService redisJobStateService) {

        this.jobRepository = jobRepository;
        this.kafkaJobProducer = kafkaJobProducer;
        this.redisJobStateService = redisJobStateService;
    }

    @Scheduled(fixedDelayString = "${job.scheduler.poll-interval-ms:1000}")
    @Transactional
    public void enqueueDueJobs() {

        LocalDateTime now = LocalDateTime.now();

        List<Job> dueJobs =
                jobRepository
                        .findTop100ByStatusAndScheduledAtLessThanEqualOrderByScheduledAtAsc(
                                JobStatus.SCHEDULED,
                                now
                        );

        for (Job job : dueJobs) {

            int updated = jobRepository.markQueuedIfDue(
                    job.getId(),
                    JobStatus.SCHEDULED,
                    JobStatus.QUEUED,
                    now
            );

            if (updated == 1) {

                redisJobStateService.setJobStatus(
                        job.getId(),
                        JobStatus.QUEUED.name()
                );

                kafkaJobProducer.sendJob(job.getId());

                System.out.println(
                        "Job queued for execution: " + job.getId()
                );
            }
        }
    }
}
