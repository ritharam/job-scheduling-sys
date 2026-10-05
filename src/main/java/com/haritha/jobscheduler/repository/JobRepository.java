package com.haritha.jobscheduler.repository;

import com.haritha.jobscheduler.entity.Job;
import com.haritha.jobscheduler.entity.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface JobRepository extends JpaRepository<Job, Long> {

    List<Job> findTop100ByStatusAndScheduledAtLessThanEqualOrderByScheduledAtAsc(
            JobStatus status,
            LocalDateTime scheduledAt
    );

    @Transactional
    @Modifying
    @Query("""
            update Job j
            set j.status = :queuedStatus
            where j.id = :id
              and j.status = :scheduledStatus
              and j.scheduledAt <= :now
            """)
    int markQueuedIfDue(
            @Param("id") Long id,
            @Param("scheduledStatus") JobStatus scheduledStatus,
            @Param("queuedStatus") JobStatus queuedStatus,
            @Param("now") LocalDateTime now
    );

    @Transactional
    @Modifying
    @Query("""
            update Job j
            set j.status = :runningStatus,
                j.startedAt = :startedAt,
                j.workerId = :workerId
            where j.id = :id
              and j.status = :queuedStatus
            """)
    int claimQueuedJob(
            @Param("id") Long id,
            @Param("queuedStatus") JobStatus queuedStatus,
            @Param("runningStatus") JobStatus runningStatus,
            @Param("startedAt") LocalDateTime startedAt,
            @Param("workerId") String workerId
    );

    @Transactional
    @Modifying
    @Query("""
            update Job j
            set j.status = :cancelledStatus,
                j.completedAt = :completedAt,
                j.errorMessage = :message
            where j.id = :id
              and j.status in :cancellableStatuses
            """)
    int cancelIfCancellable(
            @Param("id") Long id,
            @Param("cancelledStatus") JobStatus cancelledStatus,
            @Param("completedAt") LocalDateTime completedAt,
            @Param("message") String message,
            @Param("cancellableStatuses") List<JobStatus> cancellableStatuses
    );

}
