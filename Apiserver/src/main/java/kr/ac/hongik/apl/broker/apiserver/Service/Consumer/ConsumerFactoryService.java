package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.BufferedConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ImmediateConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * Consumer 를 동적으로 생성하기 위한 Factory Service 임.
 * Consumer 생성을 위해 필요한, 변할리 없는 각종 configs 들을 AutoWired 하고
 * 변할 예정인 Configs 들은 parameter로 받아 그 내용을 적용시킵니다.
 * return 을 bufferedConsumingPbftClient(혹은 Imme) 로 하였음.
 * 이 서비스는 아래의 클래스에서 활용함.
 * @see kr.ac.hongik.apl.broker.apiserver.Controller.KafkaAdminController#createBuffConsumer(ConsumerBufferConfigs)
 *
 * @Author 최상현
 */
@Slf4j
@Service
@Getter
@Setter
public class ConsumerFactoryService {

    private Map<String, Object> consumerConfigs;
    private Map<String, Object> bufferedClientConfigs;
    private Map<String, Object> immediateConsumerConfigs;
    private final Properties pbftClientProperties;
    public final HashMap<String, Object> esRestClientConfigs;
    private final AsyncExecutionService asyncExecutionService;
    private final ObjectMapper objectMapper;
    private final ConsumerDataService consumerDataService;
    private final SendAckToDesignatedURLService sendAckToDesignatedURLService;

    @Autowired
    public ConsumerFactoryService(Properties pbftClientProperties, HashMap<String, Object> esRestClientConfigs,
                                  AsyncExecutionService asyncExecutionService, ObjectMapper objectMapper,
                                  ConsumerDataService consumerDataService, SendAckToDesignatedURLService sendAckToDesignatedURLService) {

        this.pbftClientProperties = pbftClientProperties;
        this.esRestClientConfigs = esRestClientConfigs;
        this.asyncExecutionService = asyncExecutionService;
        this.objectMapper = objectMapper;
        this.consumerDataService = consumerDataService;
        this.sendAckToDesignatedURLService = sendAckToDesignatedURLService;
    }


    public BufferedConsumingPbftClient MakeBufferedConsumer(Map<String, Object> consumerConfigs,Map<String, Object> bufferedClientConfigs) {
        BufferedConsumingPbftClient bufferedConsumingPbftClient = new BufferedConsumingPbftClient(consumerConfigs,bufferedClientConfigs,
                pbftClientProperties,esRestClientConfigs,asyncExecutionService,objectMapper,consumerDataService, sendAckToDesignatedURLService);

        return  bufferedConsumingPbftClient;
    }
    public ImmediateConsumingPbftClient MakeImmediateConsumer(Map<String, Object> consumerConfigs,Map<String, Object> immediateConsumerConfigs) {
        ImmediateConsumingPbftClient immediateConsumingPbftClient = new ImmediateConsumingPbftClient(consumerConfigs,immediateConsumerConfigs,
                pbftClientProperties,esRestClientConfigs,asyncExecutionService,objectMapper,consumerDataService);

        return immediateConsumingPbftClient;
    }
}
