package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerData;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumingPbftClient;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@ToString
@Getter
public class ConsumerDataService {
    private ConcurrentHashMap<String, ConsumerData> consumerDataMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConsumingPbftClient> consumerMap = new ConcurrentHashMap<>();

    public void setData(String subscription, int timeout, int minBatchSize) {
        ConsumerData consumerData = new ConsumerData(timeout,minBatchSize);

        if (!checkTopic(subscription)) {
            this.consumerDataMap.put(subscription, consumerData);

            log.trace("checking add func:"+ consumerDataMap.keySet()+
                    consumerDataMap.get(subscription).getTimeout()+
                    consumerDataMap.get(subscription).getMinbatch());
        }
    }

    public void setData(String subscription, int timeout) {
        ConsumerData consumerData = new ConsumerData(timeout, 0);

        if (!checkTopic(subscription)) {
            this.consumerDataMap.put(subscription, consumerData);

            log.trace("checking add func:"+ consumerDataMap.keySet()+
                    consumerDataMap.get(subscription).getTimeout()+
                    consumerDataMap.get(subscription).getMinbatch());
        }
    }

    public void setConsumer(String subscription, ConsumingPbftClient consumingPbftClient){
        if(!checkTopic((subscription))){
            this.consumerMap.put(subscription,consumingPbftClient);
        }
    }

    public boolean checkTopic(String topic) {
        return this.consumerDataMap.containsKey(topic);
    }

    public void deleteData(String subscription) {
        if(this.consumerDataMap.containsKey(subscription))
        {
            this.consumerDataMap.remove(subscription);
            log.info(String.format("%s's data has removed", subscription));
        }
    }
}
