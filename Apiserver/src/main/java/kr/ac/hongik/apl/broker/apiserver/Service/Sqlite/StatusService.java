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

    @Autowired
    StatusMapper statusMapper;

    ObjectMapper om = new ObjectMapper();

    public List<ConsumerBufferConfigs> getBufferStatus() throws JsonProcessingException {
      List<DBConsumerBufferConfigs> dbc = statusMapper.selectAllBufferStatus();

      return dbc.stream()
              .map(dbObj -> dbObj.toObject())
              .collect(Collectors.toList());
    }

    public void addBufferStatus(ConsumerBufferConfigs status) throws JsonProcessingException {
        List<String> jsonList= BuffInsert(status);
        statusMapper.insertBufferStatus(status, jsonList.get(0), jsonList.get(1));
    }

    public void deleteBufferStatus(ConsumerBufferConfigs status) throws JsonProcessingException {
        statusMapper.deleteBufferStatus(BuffDelete(status));
    }

    public List<ConsumerImmediateConfigs> getImmediateStatus() throws JsonProcessingException {
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

    public void addImmediateStatus(ConsumerImmediateConfigs status) throws JsonProcessingException {
        List<String> jsonList= ImmeInsert(status);
        statusMapper.insertImmediateStatus(status, jsonList.get(0), jsonList.get(1));
    }

    public void deleteImmediateStatus(ConsumerImmediateConfigs status) throws JsonProcessingException {
        statusMapper.deleteImmediateStatus(ImmeDelete(status));
    }

    //json화해서 insert하기위함-buffer
    private List<String> BuffInsert(ConsumerBufferConfigs consumerBufferConfigs) throws JsonProcessingException {
        List<String> jsonList = new ArrayList<>();

        jsonList.add(om.writeValueAsString(consumerBufferConfigs.getBootstrapServersConfig()));
        jsonList.add(om.writeValueAsString(consumerBufferConfigs.getBuffTopicName()));

        return jsonList;
    }

    //json화해서 insert하기위함-immediate
    private List<String> ImmeInsert(ConsumerImmediateConfigs consumerImmediateConfigs) throws JsonProcessingException {
        List<String> jsonList = new ArrayList<>();

        jsonList.add(om.writeValueAsString(consumerImmediateConfigs.getBootstrapServersConfig()));
        jsonList.add(om.writeValueAsString(consumerImmediateConfigs.getImmediateTopicName()));

        return jsonList;
    }

    //json화해서 delete하기 위함 -buffer
    private String BuffDelete(ConsumerBufferConfigs consumerBufferConfigs) throws JsonProcessingException {
        String jsonString = om.writeValueAsString(consumerBufferConfigs.getBuffTopicName());
        return jsonString;
    }

    //json화해서 delete하기 위함 -immediate
    private String ImmeDelete(ConsumerImmediateConfigs consumerImmediateConfigs) throws JsonProcessingException {
        String jsonString = om.writeValueAsString(consumerImmediateConfigs.getImmediateTopicName());
        return jsonString;
    }
}

