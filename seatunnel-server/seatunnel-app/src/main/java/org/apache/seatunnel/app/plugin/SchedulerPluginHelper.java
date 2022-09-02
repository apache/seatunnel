package org.apache.seatunnel.app.plugin;

import org.apache.seatunnel.scheduler.api.IInstanceService;
import org.apache.seatunnel.scheduler.api.IJobService;
import org.apache.seatunnel.scheduler.api.ISchedulerManager;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;

@Configuration
public class SchedulerPluginHelper {

    private final ISchedulerManager schedulerManager;

    public SchedulerPluginHelper(ISchedulerManager schedulerManager) {
        this.schedulerManager = schedulerManager;
    }

    @Bean
    public IJobService getJobService() {
        return schedulerManager.getJobService();
    }

    @Bean
    public IInstanceService getInstanceService() {
        return schedulerManager.getInstanceService();
    }

    @PreDestroy
    public void close() throws Exception {
        schedulerManager.close();
    }
}
