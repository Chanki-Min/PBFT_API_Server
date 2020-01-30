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
    ConcurrentHashMap<String, ConsumerResult> ConsumerDataMap = new ConcurrentHashMap<>();
    public void setData(String subscription, String name,Integer timeout, Integer minbatch){
        ConsumerResult consumerResult = new ConsumerResult(name,timeout,minbatch);
        if (!checkTopic(subscription)){
            this.ConsumerDataMap.put(subscription.toString(),consumerResult);
            log.info("checking add func:"+ConsumerDataMap.keySet()+ConsumerDataMap.get(subscription).getName()+ConsumerDataMap.get(subscription).getTimeout()+
                    ConsumerDataMap.get(subscription).getMinbatch());
        }
    }
//TODO :: { topic , (타임아웃, 배치 사이즈) } 형태로 받아야 함.
    public boolean checkTopic(String topic){
        return this.ConsumerDataMap.containsKey(topic);
    }

    public void deleteData(String subs){
        if(this.ConsumerDataMap.containsKey(subs))
        {
            this.ConsumerDataMap.remove(subs);
            log.info(subs+"'s data is removed.");
        }
    }

}
