package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@Getter
public class ConsumerDataService {
    private ConcurrentHashMap<String, ConsumerResult> ConsumerDataMap = new ConcurrentHashMap<>();

    public void setData(String subscription, int timeout, int minBatchSize) {
        ConsumerResult consumerResult = new ConsumerResult(timeout,minBatchSize);

        if (!checkTopic(subscription)) {
            this.ConsumerDataMap.put(subscription,consumerResult);

            log.trace("checking add func:"+ConsumerDataMap.keySet()+
                    ConsumerDataMap.get(subscription).getTimeout()+
                    ConsumerDataMap.get(subscription).getMinbatch());
        }
    }

    public void setData(String subscription, int timeout) {
        ConsumerResult consumerResult = new ConsumerResult(timeout, 0);

        if (!checkTopic(subscription)) {
            this.ConsumerDataMap.put(subscription,consumerResult);

            log.trace("checking add func:"+ConsumerDataMap.keySet()+
                    ConsumerDataMap.get(subscription).getTimeout()+
                    ConsumerDataMap.get(subscription).getMinbatch());
        }
    }

    public boolean checkTopic(String topic) {
        return this.ConsumerDataMap.containsKey(topic);
    }

    public void deleteData(String subscription) {
        if(this.ConsumerDataMap.containsKey(subscription))
        {
            this.ConsumerDataMap.remove(subscription);
            log.info(String.format("%s's data has removed", subscription));
        }
    }
}
