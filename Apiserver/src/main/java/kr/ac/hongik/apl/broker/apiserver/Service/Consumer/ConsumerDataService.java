package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerData;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumingPbftClient;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@ToString
@Getter
public class ConsumerDataService {
    private ConcurrentHashMap<String, ConsumingPbftClient> consumerMap = new ConcurrentHashMap<>();

    public void setConsumer(String topicName, ConsumingPbftClient consumingPbftClient){
        this.consumerMap.put(topicName,consumingPbftClient);
        log.info(String.format("%s's data has added", topicName));
    }

    public boolean checkTopic(String topicName) {
        return this.consumerMap.containsKey(topicName);
    }

    public void consumerShutdown(String topicName) throws Exception {
        this.consumerMap.get(topicName).destroy();
        log.info("Request - shutdown consumer : " + topicName);
    }

    public void deleteData(String topicName) {
        this.consumerMap.remove(topicName);
        log.info(String.format("%s's data has removed", topicName));
    }

    public Map<String, ConsumerData> getConsumerList(){
        Map<String, ConsumerData> consumerList = new HashMap<String, ConsumerData>();

        for(Map.Entry<String, ConsumingPbftClient> e : consumerMap.entrySet()) {
            consumerList.put(e.getKey(),e.getValue().getConsumerData());
        }

        return consumerList;
    }
}
