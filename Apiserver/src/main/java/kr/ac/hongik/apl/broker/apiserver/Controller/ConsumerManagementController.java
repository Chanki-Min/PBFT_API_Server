package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumingPbftClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//TODO : bufferedconsumer만 꺼지도록 구현
@RestController(value = "/consumer")
public class ConsumerManagementController {
    @Autowired
    ConsumingPbftClient bufferedConsumingPbftClient;

    @Autowired
    ConsumingPbftClient immediateConsumingPbftClient;


    @RequestMapping(value = "/consumer/shutdown")
    @ResponseBody
    public String shutdown() throws Exception {

        bufferedConsumingPbftClient.destroy();
        immediateConsumingPbftClient.destroy();
        return "shutdown";
    }

    /**
     * @param topicName
     * @param minBatchSize
     * @param timeout
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/consumer/changesettings")
    @ResponseBody
    public String changeConsumerSettings(@RequestParam(value = "topicName", required = true, defaultValue = "hi") String topicName,
                                         @RequestParam(value = "minBatchSize", required = true, defaultValue = "600") int minBatchSize,
                                         @RequestParam(value = "timeout", required = true, defaultValue = "5000") int timeout) throws Exception {

        bufferedConsumingPbftClient.acceptConsumerSettings(topicName, minBatchSize, timeout);
        immediateConsumingPbftClient.acceptConsumerSettings(topicName, minBatchSize, timeout);
        return "shutdown";
    }
}
