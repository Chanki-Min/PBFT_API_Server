package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumingPbftClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 컨슈머 객체를 종료시키는 메소드들
 * 1. shutdownBuffer 메소드와 shutdownImmediate 메소드는
 * 각각 클라이언트의 들어온 요청에 대해 해당 컨슈머 서비스만을 종료시킨다.
 * <p>
 * 2. changeConsumerSettings 메소드는 bufferedConsumer 서브스에만 해당하는 메소드이며
 * 인자로 topicName, minBatchSize, timeout 을 받아 acceptConsumerSettings 메소드에 넘겨준다.
 * <p>
 * immediateConsumer에 위 메서드가 없는 이유는
 * immediateConsumer 서비스는 컨슈머의 설정들이 바뀔 일이 없기 때문이다.
 */
@RestController(value = "/consumer")
public class ConsumerManagementController {
    @Autowired
    ConsumingPbftClient bufferedConsumingPbftClient;

    @Autowired
    ConsumingPbftClient immediateConsumingPbftClient;


    @RequestMapping(value = "/consumer/buffer/shutdown")
    @ResponseBody
    public String shutdownBuffer() throws Exception {

        bufferedConsumingPbftClient.destroy();
        return "shutdown buffered consumer";
    }

    @RequestMapping(value = "/consumer/immediate/shutdown")
    @ResponseBody
    public String shutdownImmediate() throws Exception {

        immediateConsumingPbftClient.destroy();
        return "shutdown immediate consumer";
    }

    @RequestMapping(value = "/consumer/buffer/changesettings")
    @ResponseBody
    public String changeConsumerSettings(@RequestParam(value = "topicName", required = true, defaultValue = "hi") String topicName,
                                         @RequestParam(value = "minBatchSize", required = true, defaultValue = "600") int minBatchSize,
                                         @RequestParam(value = "timeout", required = true, defaultValue = "5000") int timeout) throws Exception {

        bufferedConsumingPbftClient.acceptConsumerSettings(topicName, minBatchSize, timeout);
        return "changed buffered consumer's settings";
    }
}
