package com.javalover.Config;

import com.javalover.Entity.Customer;
import com.javalover.Repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class ApplicationBatchConfig {

    @Autowired
    private CustomerRepository repository;

    @Autowired
    private JobRepository jobRepository;  // Required for JobBuilderFactory and StepBuilderFactory

    @Autowired
    private PlatformTransactionManager transactionManager;  // Required for JobBuilderFactory and StepBuilderFactory

    // Define StepBuilderFactory bean
    @Bean
    public StepBuilderFactory stepBuilderFactory(JobRepository jobRepository) {
        return new StepBuilderFactory(jobRepository);
    }

    // Define JobBuilderFactory bean
    @Bean
    public JobBuilderFactory jobBuilderFactory() {
        return new JobBuilderFactory(jobRepository);
    }

    @Bean
    public FlatFileItemReader<Customer> itemReader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("/Users/javatechie/Downloads/study-docs/customers.csv"));
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public CustomerDataProcessor processor() {
        return new CustomerDataProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> itemWriter() {
        RepositoryItemWriter<Customer> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(repository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    @Bean
    public Step importCustomersStep() {
        return stepBuilderFactory(jobRepository).get("ImportCustomersStep").<Customer, Customer>chunk(10)
                .reader(itemReader())
                .processor(processor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory().get("importCustomersJob")
                .incrementer(new RunIdIncrementer())  // Add an incrementer for the Job
                .flow(importCustomersStep())
                .end().build();
    }
}
