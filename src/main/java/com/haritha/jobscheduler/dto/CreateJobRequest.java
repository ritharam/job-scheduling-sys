package com.haritha.jobscheduler.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;

public class CreateJobRequest {

    @NotBlank(message = "name must not be blank")
    private String name;

    @NotBlank(message = "type must not be blank")
    private String type;

    private String payload;

    @NotNull(message = "scheduledAt must not be null")
    private LocalDateTime scheduledAt;

    @Min(value = 0, message = "maxRetries must be 0 or greater")
    private Integer maxRetries = 3;

    public CreateJobRequest() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public LocalDateTime getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(LocalDateTime scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }
}
