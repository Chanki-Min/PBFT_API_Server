package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerData;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Slf4j
@Controller
public class ConsumerDataController {

    public final ConsumerDataService consumerDataService;

    @Autowired
    public ConsumerDataController(ConsumerDataService consumerDataService) {
        this.consumerDataService = consumerDataService;
    }

    //TODO : consumerDataMap 말고 consumerMap에서 가져오기
    @RequestMapping(value = "/consumer/getMetadata")
    @ResponseBody
    public Map<String, ConsumerData> getAllConsumerData() {
        return consumerDataService.getConsumerDataMap();
    }
}