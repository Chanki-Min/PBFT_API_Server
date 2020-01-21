package kr.ac.hongik.apl.broker.apiserver.Configuration;

import kr.ac.hongik.apl.broker.apiserver.Service.BufferedConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.ImmediateConsumingPbftClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@PropertySource("classpath:kafka.properties")
public class KafkaConsumerConfiguration {
    @Autowired
    Environment env;

    @Bean(name = "consumerConfigs")
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Lee");
        //props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.scheduledproducer.scheduledproducer.sensordata.Sensor");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return props;
    }

//    @Bean(name = "listenerServiceConfigs")
//    public Map<String, Object> listenerServiceConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(KafkaListenerService.TOPICS, Arrays.asList(env.getProperty(KafkaListenerService.TOPICS)));
//        props.put(KafkaListenerService.MIN_BATCH_SIZE, env.getProperty(KafkaListenerService.MIN_BATCH_SIZE));
//        return props;
//    }

    @Bean(name = "bufferClientConfigs")
    public Map<String, Object> bufferClientConfigs() {
        Map<String, Object> props = new HashMap<>();
        log.info("Buff con start");
        props.put(BufferedConsumingPbftClient.TOPICS, Arrays.asList(env.getProperty(BufferedConsumingPbftClient.TOPICS)));
        props.put(BufferedConsumingPbftClient.MIN_BATCH_SIZE, env.getProperty(BufferedConsumingPbftClient.MIN_BATCH_SIZE));
        return props;
    }
    @Bean(name = "ImmediateClientConfigs")
    public Map<String, Object> ImmediateServiceConfigs() {
        Map<String, Object> props = new HashMap<>();
        log.info("Imme con start");
        props.put(ImmediateConsumingPbftClient.TOPICS, Arrays.asList(env.getProperty(ImmediateConsumingPbftClient.TOPICS)));
        props.put(ImmediateConsumingPbftClient.MIN_BATCH_SIZE, env.getProperty(ImmediateConsumingPbftClient.MIN_BATCH_SIZE));
        return props;
    }
}