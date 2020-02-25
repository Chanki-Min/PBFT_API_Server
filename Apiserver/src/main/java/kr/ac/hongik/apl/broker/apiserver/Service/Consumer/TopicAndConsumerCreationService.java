package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import kr.ac.hongik.apl.broker.apiserver.Pojo.BufferedConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ImmediateConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@Slf4j
@Service
public class TopicAndConsumerCreationService {
    private final ConsumerFactoryService consumerFactoryService;
    private final AsyncExecutionService asyncExecutionService;
    private final ConsumerDataService consumerDataService;
    private final StatusService statusService;

    @Autowired
    public TopicAndConsumerCreationService(ConsumerFactoryService consumerFactoryService, AsyncExecutionService asyncExecutionService, ConsumerDataService consumerDataService, StatusService statusService) {
        this.consumerFactoryService = consumerFactoryService;
        this.asyncExecutionService = asyncExecutionService;
        this.consumerDataService = consumerDataService;
        this.statusService = statusService;
    }

    /**
     * topic을 생성하고 생성한 topic의 정보를 출력한다.
     *
     * @param newTopic topic생성을 위한 정보를 가진 객체, @KafkaAdminController에서 TopicConsumerInfo객체의 getNewTopic()으로 넘겨준다.
     * @param toAdminConfigs bootstrap-server의 주소, @KafkaAdminController에서 TopicConsumerInfo객체의 toAdminConfigs()으로 넘겨준다.
     * @author 이지민
     */
    public void createTopic(NewTopic newTopic, Map<String, Object> toAdminConfigs) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(toAdminConfigs);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            result.values().get(newTopic.name()).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("",e);
            throw e;
        }
    }

    /**
     * request 로 컨슈머 정보가 api에 전달을 받으면
     * 1. 토픽 리스트의 사이즈를 확인 한다.
     *  컨슈머는 오직 하나의 토픽만을 구독하기로 약속되어 있기 때문에
     *  토픽 사이즈가 1, 즉 한개의 토픽 정보가 있을 때만 다음 단계로 넘어가고
     *  그렇지 않으면 토픽을 확인하라는 메시지를 전달한다.
     * 2. 토픽이 이미 다른 컨슈머에 의해 구독되어 있는지를 확인한다.
     *  토픽 리스트에 한개의 토픽만이 있기 때문에, 0번째 토픽을 가져와 이미 생성된 컨슈머들이
     *  담긴 컨슈머맵에서 해당 토픽을 구독하는 컨슈머가 존재하는지 여부를 확인한다.
     *  컨슈머맵에 같은 토픽을 구독하는 컨슈머가 있으면 컨슈머를 생성하지 않고
     *  토픽을 확인하라는 메시지를 전달한다.
     * 3. 위 두 조건이 만족되면 consumerFactoryService 로 컨슈머를 생성하고 컨슈머를
     *  컨슈머 정보를 담고 있는 consumerMap 에 토픽 이름을 키로 하여 삽입한다.
     * 4. 컨슈머의 서비스를 비동기로 실행한다.
     *
     * @param configs api 사용자가 정의한 컨슈머 config 정보가 담긴 객체
     * @return
     *
     * @author 이혁수, 최상현
     */
    public void createBufferConsumer(ConsumerBufferConfigs configs) {
        if(!configs.validateMemberVar())
        {
           throw new IllegalArgumentException("missing essential fields");
        }
        int topicListSize = configs.getBuffTopicName().size();
        if (topicListSize == 1) {
            String topicName = configs.getBuffTopicName().get(0);
            if (!consumerDataService.checkTopic(topicName)) {
                BufferedConsumingPbftClient bufferedConsumingPbftClient =
                        consumerFactoryService.MakeBufferedConsumer(configs.getCommonConfigs(), configs.getBuffConfigs());
                statusService.addBufferStatus(configs);
                consumerDataService.setConsumer(topicName,bufferedConsumingPbftClient);
                asyncExecutionService.runAsConsumerExecutor(bufferedConsumingPbftClient::startConsumer);
                log.info(String.format("New buffer consumer added : %s", configs.toString()));
            } else {
                throw new IllegalArgumentException(String.format("Consumer with topic : %s is already exists", topicName));
            }
        } else {
            throw new IllegalArgumentException("Topic list's size should be 1");
        }
    }

    public void createImmediateConsumer(ConsumerImmediateConfigs configs) {
        if(!configs.validateMemberVar())
        {
            throw new IllegalArgumentException("missing essential fields");
        }
        int topicListSize = configs.getImmediateTopicName().size();
        if (topicListSize == 1) {
            String topicName = configs.getImmediateTopicName().get(0);
            if (!consumerDataService.checkTopic(topicName)) {
                ImmediateConsumingPbftClient immediateConsumingPbftClient =
                        consumerFactoryService.MakeImmediateConsumer(configs.getCommonConfigs(), configs.getImmeConfigs());
                statusService.addImmediateStatus(configs);
                consumerDataService.setConsumer(topicName,immediateConsumingPbftClient);
                asyncExecutionService.runAsConsumerExecutor(immediateConsumingPbftClient::startConsumer);
                log.info(String.format("New immediate consumer added : %s", configs.toString()));
            } else {
                throw new IllegalArgumentException(String.format("Consumer with Topic : %s is already exists", topicName));
            }
        } else {
            throw new IllegalArgumentException("Topic list's size should be 1");
        }
    }
}
