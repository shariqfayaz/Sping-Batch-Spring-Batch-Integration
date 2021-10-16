package com.SpringBatchIntegration.SpringIntegrationConfig;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;

import java.io.File;

public class IntegrationConfig {
    @Autowired
    private Job sampleJob;

    @Bean
    public JobLauncher getJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        return jobLauncher;
    }

    protected DirectChannel inputChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow sampleFlow() {
        return IntegrationFlows
                .from(fileReadingMessageSource(), c -> c.poller(Pollers.fixedDelay(1000)))//
                .channel(inputChannel())
                .transform(fileMessageToJobRequest1())
                .handle(jobLaunchingMessageHandler())
                .get();

    }

    @Bean
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File("C:/Users/LENOVO/Desktop/csvFiles"));
        source.setFilter(new SimplePatternFileListFilter("*.psv"));
        source.setUseWatchService(true);
        source.setWatchEvents(FileReadingMessageSource.WatchEventType.CREATE);

        return source;
    }

    @Bean
    FileMessageToJobRequest fileMessageToJobRequest1() {
        FileMessageToJobRequest transformer = new FileMessageToJobRequest();
        transformer.setJob(sampleJob);
        transformer.setFileParameterName("file_path");
        return transformer;
    }

    @Bean
    JobLaunchingMessageHandler jobLaunchingMessageHandler() {
        JobLaunchingMessageHandler handler = new JobLaunchingMessageHandler(getJobLauncher());
        return handler;
    }
}
