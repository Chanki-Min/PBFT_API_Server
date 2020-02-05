package kr.ac.hongik.apl.broker.apiserver.Pojo;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs.getStringObjectMap;

/**
 * ConsumerImmdiateConfigs 객체와 용도가 동일한 POJO 임.
 * API 서버 Consumer를 생성하는 함수 RequestBody 의 parameter가 될 객체임.
 * Consumer 객체 생성에 필요한 공통 설정을 commonConfigs Map 객체에 대입하고 반환,
 * Buffer기능을 수행하고 그 consumer를 생성하는데 필요한 bufferConfigs Map 객체를 반환하는 것이 객체의 최종역할임.
 *
 * @author 최상현
 */
public class ConsumerBufferConfigs {
    public static final String BUFFERED_CONSUMER_TOPICS = "kafka.listener.service.topic";
    public static final String BUFFERED_CONSUMER_MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
    public static final String BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.isHashListInclude";
    public static final String BUFFERED_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.timeout.millis";
    public static final String BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.poll.interval.millis";
    // common configs
    List<String> bootstrapServersConfig;
    Boolean autoCommitConfig;
    String groupIdConfig;
    Object keyDeserializerClassConfig;
    Object valuesDeserializerClassConfig;
    // buffer consumer configs
    List<String> buffTopicName;
    int buffMinBatchSize;
    Boolean buffIsHashListInclude;
    int buffTimeoutMillis;
    long buffPollIntervalMillis;

    /**
     * @param bootstrapServersConfig
     * @param autoCommitConfig
     * @param groupIdConfig
     * @param buffTopicName
     * @param buffMinBatchSize
     * @param buffIsHashListInclude
     * @param buffTimeoutMillis
     * @param buffPollIntervalMillis
     * Allargs 어노테이션을 활용하지않고 생성자를 지정한 이유는
     * keyDeserializeClassConfig와 valuesDeserializerClassConfig 는 JSON 으로 받기엔
     * 힘든 class 객체를 참조하여야하기 때문임.
     * 이 사항은 ConsumerImmediateConfigs 에서도 동일.
     *
     */
    public ConsumerBufferConfigs(List<String> bootstrapServersConfig, Boolean autoCommitConfig, String groupIdConfig,
                                 List<String> buffTopicName, int buffMinBatchSize,
                                 Boolean buffIsHashListInclude, int buffTimeoutMillis, long buffPollIntervalMillis) {
        this.bootstrapServersConfig = bootstrapServersConfig;

        this.autoCommitConfig = autoCommitConfig;
        this.groupIdConfig = groupIdConfig;
        this.keyDeserializerClassConfig = StringDeserializer.class;
        this.valuesDeserializerClassConfig = JsonDeserializer.class;
        this.buffTopicName = buffTopicName;
        this.buffMinBatchSize = buffMinBatchSize;
        this.buffIsHashListInclude = buffIsHashListInclude;
        this.buffTimeoutMillis = buffTimeoutMillis;
        this.buffPollIntervalMillis = buffPollIntervalMillis;
    }

    /**
     * @return getStringObjectMap 은 ConsumerImmediateConfigs 에 선언되어있는 함수임.
     * commonConfigs 는 두 객체 모두 동일한 내용을 담는 중복 함수이기 때문에 이렇게 하였음.
     */
    public Map<String, Object> getCommonConfigs() {
        return getStringObjectMap(bootstrapServersConfig, autoCommitConfig, groupIdConfig, keyDeserializerClassConfig, valuesDeserializerClassConfig);
    }

    public Map<String, Object> getBuffConfigs() {
        Map<String, Object> buffConfigs = new HashMap<>();
        buffConfigs.put(BUFFERED_CONSUMER_TOPICS,buffTopicName);
        buffConfigs.put(BUFFERED_CONSUMER_MIN_BATCH_SIZE,buffMinBatchSize);
        buffConfigs.put(BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE,buffIsHashListInclude);
        buffConfigs.put(BUFFERED_CONSUMER_TIMEOUT_MILLIS,buffTimeoutMillis);
        buffConfigs.put(BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS,buffPollIntervalMillis);
        return buffConfigs;
    }
}
