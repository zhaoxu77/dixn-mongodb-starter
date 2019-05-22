package com.dixn.zookeeper.annotation;

import com.dixn.zookeeper.config.MongodbConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(MongodbConfiguration.class)
public @interface EnableMongodbConfiguration {
}
