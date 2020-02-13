package kr.ac.hongik.apl.broker.apiserver.Pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Data
public class DBConsumerBufferConfigs {

    ObjectMapper om = new ObjectMapper();

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

    //db에서 가져올 때
    public DBConsumerBufferConfigs(String bootstrapServersConfig, Boolean autoCommitConfig, String groupIdConfig, String jsonTopicName, int buffMinBatchSize, Boolean buffIsHashListInclude, int buffTimeoutMillis, long buffPollIntervalMillis) throws JsonProcessingException {
        this.bootstrapServersConfig = om.readValue(bootstrapServersConfig, new TypeReference<List<String>>(){});
        this.autoCommitConfig = autoCommitConfig;
        this.groupIdConfig = groupIdConfig;
        this.buffTopicName = om.readValue(jsonTopicName, new TypeReference<List<String>>(){});
        this.buffMinBatchSize = buffMinBatchSize;
        this.buffIsHashListInclude = buffIsHashListInclude;
        this.buffTimeoutMillis = buffTimeoutMillis;
        this.buffPollIntervalMillis = buffPollIntervalMillis;
        this.keyDeserializerClassConfig = StringDeserializer.class;
        this.valuesDeserializerClassConfig = JsonDeserializer.class;

    }


    public ConsumerBufferConfigs toObject() {
        return new ConsumerBufferConfigs(bootstrapServersConfig, autoCommitConfig, groupIdConfig, buffTopicName, buffMinBatchSize, buffIsHashListInclude, buffTimeoutMillis,buffPollIntervalMillis);
    }
}
