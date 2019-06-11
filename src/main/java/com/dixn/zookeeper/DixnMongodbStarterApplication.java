package com.dixn.zookeeper;

import com.dixn.zookeeper.service.MongoService;
import org.bson.Document;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@SpringBootApplication
public class DixnMongodbStarterApplication {



    public static void main(String[] args) {
//        SpringApplication.run(DixnMongodbStarterApplication.class, args);

        ConfigurableApplicationContext context = new SpringApplicationBuilder(DixnMongodbStarterApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);

        /*String helloWorld = context.getBean("helloWorld", String.class);
        System.out.println("helloWorld:" + helloWorld);*/

        MongoService mongoService = context.getBean("mongoService", MongoService.class);

        try {
            List<Document> list = mongoService.findAll("device_realtime");
            System.out.println(list.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
