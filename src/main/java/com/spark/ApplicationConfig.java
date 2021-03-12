package com.spark;

import com.spark.spark_config.SparkConfigurator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfig {
    /*@Bean
    public SparkConfigurator sparkSession() {
        return new SparkConfigurator();
    }*/
}
