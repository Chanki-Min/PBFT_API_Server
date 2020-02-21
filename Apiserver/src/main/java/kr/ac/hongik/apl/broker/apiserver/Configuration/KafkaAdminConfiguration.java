package kr.ac.hongik.apl.broker.apiserver.Configuration;


import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfiguration {

    private Environment env;

    @Autowired
    public KafkaAdminConfiguration(Environment env) {
        this.env = env;
    }

    @Bean
    public Map<String, Object> adminConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        return props;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(adminConfigs());
    }

}
