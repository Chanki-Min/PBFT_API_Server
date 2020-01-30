package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerResult;
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

    @Autowired
    public ConsumerDataService consumerDataService;


    @RequestMapping(value = "/consumer/getMetadata")
    @ResponseBody
    public Map<String, ConsumerResult> getAllConsumerData(){
        return consumerDataService.getConsumerDataMap();
    }
}
