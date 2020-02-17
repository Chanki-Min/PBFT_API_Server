package kr.ac.hongik.apl.broker.apiserver.Service.EventListener;


import kr.ac.hongik.apl.broker.apiserver.Pojo.BufferedConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ImmediateConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerFactoryService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 각 Event 에 맞는 적절한 핸들링을 위해 Event 종류에 따른 핸들러 메소드를 선언하였다.
 * 해당 이슈의 경우에는 Bean 주입 이후에 실행되어야할 로직이므로 ContextStartedEvent 가 발생할 경우의
 * 핸들러에 해당 로직을 구현하였다. 이 후에도 다양한 Event 에 대해서도 테스트 혹은 대처할 수 있도록 다른 리스너들도
 * 껍데기를 만들어 두었다.
 *
 * @author 최상현
 */
@Slf4j
@Component
public class ContextStartedEventListener {

    private final StatusService statusService;
    private final ConsumerFactoryService consumerFactoryService;
    private final AsyncExecutionService asyncExecutionService;
    private final ConsumerDataService consumerDataService;

    @Autowired
    public ContextStartedEventListener(StatusService statusService, ConsumerFactoryService consumerFactoryService, AsyncExecutionService asyncExecutionService, ConsumerDataService consumerDataService) {
        this.statusService = statusService;
        this.consumerFactoryService = consumerFactoryService;
        this.asyncExecutionService = asyncExecutionService;
        this.consumerDataService = consumerDataService;
    }

    /**
     * @param event Spring bean들이 init 된 이후 발생하는 ContextStartedEvent
     * DB로부터 저장되어있는 켜져야 할 Consumer 객체들의 Config 를 불러와 factoryService 를 통해 생산해낸다.
     * 이후 해당 Consumer 들을 실행한다.
     */
    @EventListener
    @Async
    public void restoreStatusFromDB(ContextStartedEvent event){

        log.info(Thread.currentThread().toString());
        log.info("ContextStartedEvent");
        List<ConsumerBufferConfigs> bufferStatusList = statusService.getBufferStatus();

        for( ConsumerBufferConfigs config : bufferStatusList){
            log.info(config.getBuffTopicName().get(0)+" is turned ON");
            BufferedConsumingPbftClient bufferedConsumingPbftClient =
                    consumerFactoryService.MakeBufferedConsumer(config.getCommonConfigs(),config.getBuffConfigs());
            consumerDataService.setConsumer(config.getBuffTopicName().get(0),bufferedConsumingPbftClient);
            log.info(config.getBuffConfigs().toString());
            asyncExecutionService.runAsConsumerExecutor(bufferedConsumingPbftClient::startConsumer);
        }

        List<ConsumerImmediateConfigs> immediateStatusList = statusService.getImmediateStatus();

        for( ConsumerImmediateConfigs config : immediateStatusList){
            log.info(config.getImmediateTopicName().get(0)+" is turned ON");
            ImmediateConsumingPbftClient immediateConsumingPbftClient =
                    consumerFactoryService.MakeImmediateConsumer(config.getCommonConfigs(),config.getImmeConfigs());
            consumerDataService.setConsumer(config.getImmediateTopicName().get(0),immediateConsumingPbftClient);
            log.info(config.getImmeConfigs().toString());
            asyncExecutionService.runAsConsumerExecutor(immediateConsumingPbftClient::startConsumer);
        }
    }
}
