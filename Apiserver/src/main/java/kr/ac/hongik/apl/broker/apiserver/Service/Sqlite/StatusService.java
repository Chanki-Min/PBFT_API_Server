package kr.ac.hongik.apl.broker.apiserver.Service.Sqlite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.DBConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.DBConsumerImmediateConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class StatusService {

    private final StatusMapper statusMapper;

    ObjectMapper om = new ObjectMapper();

    @Autowired
    public StatusService(StatusMapper statusMapper) {
        this.statusMapper = statusMapper;
    }

    public List<ConsumerBufferConfigs> getBufferStatus() {
      List<DBConsumerBufferConfigs> dbc = statusMapper.selectAllBufferStatus();

      return dbc.stream()
              .map(dbObj -> dbObj.toObject())
              .collect(Collectors.toList());
    }

    public void addBufferStatus(ConsumerBufferConfigs status) {
        List<String> jsonList= BuffInsert(status);
        statusMapper.insertBufferStatus(status, jsonList.get(0), jsonList.get(1));
    }

    public void deleteBufferStatus(ConsumerBufferConfigs status) {
        statusMapper.deleteBufferStatus(BuffDelete(status));
    }

    /**
     * public void deleteBufferStatus(ConsumerBufferConfigs status) 의 오버로딩 메소드임.
     */
    public void deleteBufferStatus(String topicName) {
        statusMapper.deleteBufferStatus(BuffDelete(topicName));
    }

    public List<ConsumerImmediateConfigs> getImmediateStatus() {
        List<DBConsumerImmediateConfigs> dbc = statusMapper.selectAllImmediateStatus();

        return dbc.stream()
                .map(dbObj -> {
                    try {
                        return dbObj.toObject();
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .collect(Collectors.toList());
    }

    public void addImmediateStatus(ConsumerImmediateConfigs status) {
        List<String> jsonList= ImmeInsert(status);
        statusMapper.insertImmediateStatus(status, jsonList.get(0), jsonList.get(1));
    }

    public void deleteImmediateStatus(ConsumerImmediateConfigs status) {
        statusMapper.deleteImmediateStatus(ImmeDelete(status));

    }

    public void deleteImmediateStatus(String topicName) {
        statusMapper.deleteImmediateStatus(ImmeDelete(topicName));

    }

    //json화해서 insert하기위함-buffer
    private List<String> BuffInsert(ConsumerBufferConfigs consumerBufferConfigs) {
        List<String> jsonList = new ArrayList<>();
        try {
            jsonList.add(om.writeValueAsString(consumerBufferConfigs.getBootstrapServersConfig()));
            jsonList.add(om.writeValueAsString(consumerBufferConfigs.getBuffTopicName()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return jsonList;
    }

    //json화해서 insert하기위함-immediate
    private List<String> ImmeInsert(ConsumerImmediateConfigs consumerImmediateConfigs) {
        List<String> jsonList = new ArrayList<>();

        try{
            jsonList.add(om.writeValueAsString(consumerImmediateConfigs.getBootstrapServersConfig()));
            jsonList.add(om.writeValueAsString(consumerImmediateConfigs.getImmediateTopicName()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }


        return jsonList;
    }

    //json화해서 delete하기 위함 -buffer
    private String BuffDelete(ConsumerBufferConfigs consumerBufferConfigs) {
        try{
            String jsonString = om.writeValueAsString(consumerBufferConfigs.getBuffTopicName());

            return jsonString;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 이는 deleteBufferStatus(String topicName) 와 AdminController 에서 parameter 의 data type 이 String 인 것을
     * 고려하여 만들어진 메소드임. immediate 도 이하동일.
     *
     * @param TopicName Configs 가 아닌 TopicName 객체만을 받는 것이 훨씬 간편하므로 함수 오버로딩함.
     * @return
     * @author 최상현
     */
    private String BuffDelete(String TopicName) {
        try{
            List<String> topicNameList = new ArrayList<>();
            topicNameList.add(TopicName);
            String jsonString = om.writeValueAsString(topicNameList);

            return jsonString;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    //json화해서 delete하기 위함 -immediate
    private String ImmeDelete(ConsumerImmediateConfigs consumerImmediateConfigs) {
        try {
            String jsonString = om.writeValueAsString(consumerImmediateConfigs.getImmediateTopicName());
            return jsonString;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param topicName
     *
     * private String ImmeDelete(ConsumerImmediateConfigs consumerImmediateConfigs) 의 오버로딩 메소드임.
     *
     */
    private String ImmeDelete(String topicName) {
        try {
            List<String> topicNameList = new ArrayList<>();
            topicNameList.add(topicName);
            String jsonString = om.writeValueAsString(topicNameList);
            return jsonString;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

