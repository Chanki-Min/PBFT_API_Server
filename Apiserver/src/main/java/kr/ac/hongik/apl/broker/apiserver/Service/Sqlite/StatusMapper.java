package kr.ac.hongik.apl.broker.apiserver.Service.Sqlite;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.DBConsumerImmediateConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.DBConsumerBufferConfigs;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface StatusMapper {
    List<DBConsumerBufferConfigs> selectAllBufferStatus();
    void insertBufferStatus(@Param("status") ConsumerBufferConfigs status, @Param("jsonConfigs") String jsonConfigs, @Param("jsonTopicName") String jsonTopicName);
    void deleteBufferStatus(String topicName);

    List<DBConsumerImmediateConfigs> selectAllImmediateStatus();
    void insertImmediateStatus(@Param("status") ConsumerImmediateConfigs status, @Param("jsonConfigs") String jsonConfigs, @Param("jsonTopicName") String jsonTopicName);
    void deleteImmediateStatus(String topicName);

}