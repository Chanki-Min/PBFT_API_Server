package kr.ac.hongik.apl.broker.apiserver.Pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.*;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @KafkaAdminController가 사용하기 위한 객체로
 * 토픽, 컨슈머 생성을 위한 정보를 다 받아서
 * @TopicAndConsumerCreationService의 createTopic, createImmediateConsumer method에 사용한다.
 *
 * @author 이지민
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class TopicImmediateConsumerInfo {

    public static final String IMMEDIATE_CONSUMER_TOPICS = "kafka.listener.service.immediate.topic";
    public static final String IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.immediate.isHashListInclude";
    public static final String IMMEDIATE_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.immediate.timeout.millis";
    public static final String IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.immediate.poll.interval.millis";

    //create topic
    private String topic;
    private int numpartitions;
    private short replicas;

    //consumer common configs
    private List<String> bootstrapServersConfig;
    private Boolean autoCommitConfig;
    private String groupIdConfig;
    private Object keyDeserializerClassConfig;
    private Object valuesDeserializerClassConfig;

    // immediate consumer configs
    List<String> immediateTopicName;
    Boolean immediateIsHashListInclude;
    int immediateTimeoutMillis;
    long immediatePollIntervalMillis;

    public NewTopic toNewTopic() {
        return new NewTopic(topic, numpartitions, replicas);
    }

    public ConsumerImmediateConfigs toImmediateConfigs() {
        return new ConsumerImmediateConfigs(bootstrapServersConfig, autoCommitConfig, groupIdConfig, immediateTopicName, immediateIsHashListInclude, immediateTimeoutMillis, immediatePollIntervalMillis);
    }

    public Map<String, Object> toAdminConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        return props;
    }
}
