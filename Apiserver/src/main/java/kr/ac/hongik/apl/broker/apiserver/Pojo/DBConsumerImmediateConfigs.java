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
public class DBConsumerImmediateConfigs {

    ObjectMapper om = new ObjectMapper();

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


    //db에서 가져올 때
    public DBConsumerImmediateConfigs(String bootstrapServersConfig, Boolean autoCommitConfig, String groupIdConfig, String immediateTopicName, Boolean immediateIsHashListInclude, int immediateTimeoutMillis, long immediatePollIntervalMillis) throws JsonProcessingException {
        this.bootstrapServersConfig = om.readValue(bootstrapServersConfig, new TypeReference<List<String>>(){});
        this.autoCommitConfig = autoCommitConfig;
        this.groupIdConfig = groupIdConfig;
        this.immediateTopicName = om.readValue(immediateTopicName, new TypeReference<List<String>>(){});
        this.immediateIsHashListInclude = immediateIsHashListInclude;
        this.immediateTimeoutMillis = immediateTimeoutMillis;
        this.immediatePollIntervalMillis = immediatePollIntervalMillis;
        this.keyDeserializerClassConfig = StringDeserializer.class;
        this.valuesDeserializerClassConfig = JsonDeserializer.class;
    }

    public ConsumerImmediateConfigs toObject() throws JsonProcessingException {
        return new ConsumerImmediateConfigs(bootstrapServersConfig, autoCommitConfig, groupIdConfig, immediateTopicName, immediateIsHashListInclude, immediateTimeoutMillis, immediatePollIntervalMillis);
    }


}
