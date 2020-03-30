package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.*;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @KafkaAdminController가 사용하기 위한 객체로
 * 토픽, 컨슈머 생성을 위한 정보를 다 받아서
 * @TopicAndConsumerCreationService의 createTopic, createBuffConsumer method에 사용한다.
 *
 * @author 이지민
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class TopicBufferConsumerInfo {
    public static final String BUFFERED_CONSUMER_TOPICS = "kafka.listener.service.topic";
    public static final String BUFFERED_CONSUMER_MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
    public static final String BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.isHashListInclude";
    public static final String BUFFERED_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.timeout.millis";
    public static final String BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.poll.interval.millis";

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

    // buffer consumer configs
    private int buffMinBatchSize;
    private Boolean buffIsHashListInclude;
    private int buffTimeoutMillis;
    private long buffPollIntervalMillis;


    public NewTopic toNewTopic() {
        return new NewTopic(topic, numpartitions, replicas);
    }

    public ConsumerBufferConfigs toBufferConfigs() {
        List<String> buffTopicName = new ArrayList<>(); //consumer 생성시 list<String> 형태로 변환하기 위함
        buffTopicName.add(topic);
        return new ConsumerBufferConfigs(bootstrapServersConfig, autoCommitConfig, groupIdConfig, buffTopicName, buffMinBatchSize, buffIsHashListInclude, buffTimeoutMillis, buffPollIntervalMillis);
    }

    public Map<String, Object> toAdminConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        return props;
    }

}
