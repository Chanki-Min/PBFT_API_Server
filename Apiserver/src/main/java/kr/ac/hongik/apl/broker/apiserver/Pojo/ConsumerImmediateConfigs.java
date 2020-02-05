package kr.ac.hongik.apl.broker.apiserver.Pojo;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * 자세한 사항은 ConsumerBuffConfigs 의 내용을 참고할 것
 * @see ConsumerBufferConfigs
 *
 * @Author 최상현
 */
public class ConsumerImmediateConfigs {
    public static final String IMMEDIATE_CONSUMER_TOPICS = "kafka.listener.service.immediate.topic";
    public static final String IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.immediate.isHashListInclude";
    public static final String IMMEDIATE_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.immediate.timeout.millis";
    public static final String IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.immediate.poll.interval.millis";
    // common configs
    List<String> bootstrapServersConfig;

    Boolean autoCommitConfig;
    String groupIdConfig;
    Object keyDeserializerClassConfig;
    Object valuesDeserializerClassConfig;
    // immediate consumer configs
    List<String> immediateTopicName;
    Boolean immediateIsHashListInclude;
    int immediateTimeoutMillis;
    long immediatePollIntervalMillis;

    public ConsumerImmediateConfigs(List<String> bootstrapServersConfig, Boolean autoCommitConfig, String groupIdConfig,
                                    List<String> immediateTopicName, Boolean immediateIsHashListInclude,
                                    int immediateTimeoutMillis, long immediatePollIntervalMillis) {
        this.bootstrapServersConfig = bootstrapServersConfig;

        this.autoCommitConfig = autoCommitConfig;
        this.groupIdConfig = groupIdConfig;
        this.keyDeserializerClassConfig = StringDeserializer.class;
        this.valuesDeserializerClassConfig = JsonDeserializer.class;
        this.immediateTopicName = immediateTopicName;
        this.immediateIsHashListInclude = immediateIsHashListInclude;
        this.immediateTimeoutMillis = immediateTimeoutMillis;
        this.immediatePollIntervalMillis = immediatePollIntervalMillis;
    }

    public Map<String, Object> getCommonConfigs() {
        return getStringObjectMap(bootstrapServersConfig, autoCommitConfig, groupIdConfig,
                keyDeserializerClassConfig, valuesDeserializerClassConfig);
    }

    static Map<String, Object> getStringObjectMap(List<String> bootstrapServersConfig, Boolean autoCommitConfig, String groupIdConfig,
                                                  Object keyDeserializerClassConfig, Object valuesDeserializerClassConfig) {
        Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put("bootstrap.servers", bootstrapServersConfig);

        commonConfigs.put("enable.auto.commit", autoCommitConfig);
        commonConfigs.put("group.id", groupIdConfig);
        commonConfigs.put("key.deserializer", keyDeserializerClassConfig);
        commonConfigs.put("value.deserializer", valuesDeserializerClassConfig);

        return commonConfigs;
    }

    public Map<String, Object> getConImmeConfigs() {
        Map<String, Object> immediateConfigs = new HashMap<>();
        immediateConfigs.put(IMMEDIATE_CONSUMER_TOPICS,immediateTopicName);
        immediateConfigs.put(IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE,immediateIsHashListInclude);
        immediateConfigs.put(IMMEDIATE_CONSUMER_TIMEOUT_MILLIS,immediateTimeoutMillis);
        immediateConfigs.put(IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS,immediatePollIntervalMillis);

        return immediateConfigs;
    }
}
