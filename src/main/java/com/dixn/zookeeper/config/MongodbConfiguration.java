package com.dixn.zookeeper.config;

import com.dixn.zookeeper.service.MongoService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongodbConfiguration {
    @Bean(value = "mongoService")
    public MongoService getMongoService() {
        return new MongoService();
    }
}
