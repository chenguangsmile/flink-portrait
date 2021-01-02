package com.zhangbao;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;

/**
 * @author zhangbao
 * @date 2020/11/22 0:04
 **/
@SpringBootApplication
@EnableEurekaClient
public class StartupAdmin {
    public static void main(String[] args) {
        SpringApplication.run( StartupAdmin.class, args );
    }

    //防止请求出现特殊字符，springboot2.0以上都采用tomcat8.0以上版本，对url的特殊字符有限制
    @Bean
    public ConfigurableServletWebServerFactory webServerFactory(){
        TomcatServletWebServerFactory factory= new TomcatServletWebServerFactory();
        factory.addConnectorCustomizers(connector -> connector.setProperty("relaxedQueryChars","|{}[]\\"));
        return factory;
    }

}
