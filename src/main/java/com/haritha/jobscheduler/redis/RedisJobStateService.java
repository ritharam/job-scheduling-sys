package com.haritha.jobscheduler.redis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RedisJobStateService {

    private final StringRedisTemplate redisTemplate;

    public RedisJobStateService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void setJobStatus(Long jobId, String status) {

        String key = buildKey(jobId);

        redisTemplate.opsForValue().set(
                key,
                status,
                Duration.ofHours(24)
        );
    }

    public String getJobStatus(Long jobId) {

        String key = buildKey(jobId);

        return redisTemplate.opsForValue().get(key);
    }

    public void deleteJobStatus(Long jobId) {

        String key = buildKey(jobId);

        redisTemplate.delete(key);
    }

    private String buildKey(Long jobId) {
        return "job:" + jobId + ":status";
    }
}