package kr.ac.hongik.apl.broker.apiserver.Controller;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.*;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerFactoryService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Controller
@RequestMapping("/kafka")
public class KafkaAdminController {
    /*
     * Kafka의 토픽 생성, 삭제 관리를 처리하는 KafkaAdmin 객체를 오토와이어 해줍니다
     */

    private KafkaAdmin kafkaAdmin;

    private ObjectMapper objectMapper;

    private ConsumerFactoryService consumerFactoryService;

    private AsyncExecutionService asyncExecutionService;

    private ConsumerDataService consumerDataService;

	private StatusService statusService;

	@Autowired
    public KafkaAdminController(KafkaAdmin kafkaAdmin, ObjectMapper objectMapper, ConsumerFactoryService consumerFactoryService, AsyncExecutionService asyncExecutionService, ConsumerDataService consumerDataService, StatusService statusService) {
        this.kafkaAdmin = kafkaAdmin;
        this.objectMapper = objectMapper;
        this.consumerFactoryService = consumerFactoryService;
        this.asyncExecutionService = asyncExecutionService;
        this.consumerDataService = consumerDataService;
        this.statusService = statusService;
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
     * @Author 이혁수, 최상현
     */
    @PostMapping("/consumer/create/buf")
    @ResponseBody
    public String createBuffConsumer(@RequestBody ConsumerBufferConfigs configs) {
        if(!configs.validateMemberVar())
        {
            return String.format("you've missed some of fields. try again");
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
                // TODO : client 객체 db에 삽입.
                return String.format("New Buffer Consumer added!");
            } else {
                return String.format("Check the topic name.");
            }
        } else {
            return String.format("Topic List's size should be 1.");
        }
    }

    @PostMapping("/consumer/create/imme")
    @ResponseBody
    public String createImmediateConsumer(@RequestBody ConsumerImmediateConfigs configs) {
        if(!configs.validateMemberVar())
        {
            return String.format("you've missed some of fields. try again");
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
                return String.format("New Immediate Consumer added!");
            } else {
                return String.format("Check the topic name.");
            }
        } else {
            return String.format("Topic List's size should be 1.");
        }
    }

    /**
     * 클라이언트가 가동중인 컨슈머의 minBatchSize, timeout 등 설정을 바꾸고 싶을 때 요청하는 메소드.
     * 이미 존재하는 컨슈머 설정을 동적으로 변경시키는 것이기 때문에 바꾸고자 하는 컨슈머의 토픽과 일치하여야 한다.
     * 만약에 구독하는 토픽을 변경하는 것이라면, 그것은 기존의 컨슈머를 종료하고 후에 새로운 컨슈머를 만드는 과정이여야 한다.
     * 컨슈머 생성 메서드와 같은 흐름으로 작동하며 다음과 같다.
     *
     * 1. 토픽 리스트의 사이즈가 1인지 확인
     * 2. 토픽이 이미 가동중인 컨슈머 리스트에 존재하는지 확인
     * 3. 기존의 컨슈머를 종료하며 컨슈머맵에서 삭제후
     * 4. 새로 받은 객체 정보를 통해 새로운 컨슈머를 생성 후 맵에 삽입.
     * 5. 컨슈머 서비스 가동.
     *
     * @param configs 클라이언트가 희망하는 컨슈머 config 가 담긴 객체. 이미 실행중인 컨슈머와 같은 토픽을 구독하여야 한다.
     * @return
     * @throws Exception
     *
     * @Author 이혁수
     */
    @PostMapping("/consumer/change/buf")
    @ResponseBody
    public String changeBuffConsumer(@RequestBody ConsumerBufferConfigs configs) throws Exception {
        log.info(configs.toString());
        if(!configs.validateMemberVar())
        {
            return String.format("you've missed some of fields. try again");
        }

        int topicListSize = configs.getBuffTopicName().size();
        if (topicListSize == 1) {
            String topicName = configs.getBuffTopicName().get(0);
            if (consumerDataService.checkTopic(topicName)) {
                statusService.deleteBufferStatus(configs);
                consumerDataService.consumerShutdown(topicName);
                consumerDataService.deleteData(topicName);
                BufferedConsumingPbftClient bufferedConsumingPbftClient =
                        consumerFactoryService.MakeBufferedConsumer(configs.getCommonConfigs(), configs.getBuffConfigs());
                statusService.addBufferStatus(configs);
                consumerDataService.setConsumer(topicName,bufferedConsumingPbftClient);
                asyncExecutionService.runAsConsumerExecutor(bufferedConsumingPbftClient::startConsumer);
                log.info("Buffer Consumer has been changed");
                return String.format("Buffer Consumer has been changed!");
            } else {
                return String.format("Check the topic name");
            }
        } else {
            return String.format("Topic List's size should be 1.");
        }
    }

    //TODO : 권한 인증
    @PostMapping(value = "/consumer/describe")
    @ResponseBody
    public String describeTopicRequest() {
        /*
         * 권한 인증이 필요하다면 인증 후에 현재 카프카 클러스터의 토픽 정보를 반환한다
         */
        log.info("Listing topic----------------------");
        AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());
        AtomicReference<String> mappedValue = new AtomicReference<>("");

        try {
            ListTopicsResult list = adminClient.listTopics();

            List<String> topics = new ArrayList<>(list.names().get());

            List<TopicData> topicDataList = new ArrayList<>();

            DescribeTopicsResult TopicResult = adminClient.describeTopics(topics);

            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : TopicResult.values().entrySet()) {
                TopicData topicData = new TopicData();
                topicData.setName(entry.getValue().get().name());
                topicData.setInternal(entry.getValue().get().isInternal());
                topicData.setPartitions(entry.getValue().get().partitions());
                topicDataList.add(topicData);
            }


            objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            mappedValue.set(objectMapper.writeValueAsString(topicDataList));
            System.out.println(mappedValue);

        } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
            e.printStackTrace();
        }

        return mappedValue.get();
    }

    private NewTopic getTopic(String topic, int numpartitions, short replicas) {
        return new NewTopic(topic, numpartitions, replicas);
    }

    /**
     * 가동중인 컨슈머를 종료하고 싶을 때 보내는 요청 메소드.
     * request 에 종료를 희망하는 토픽 이름을 보내면,
     * 먼저 그 이름이 가동 중인 컨슈머 정보들이 담긴 맵에 존재하는 지 확인하고
     * 존재하면 destroy() 를 통해 종료시키고 맵에서도 해당 객체를 삭제한다.
     *
     * @param topicName 종료하고자 하는 컨슈머가 구독 중인 토픽 이름.
     * @return
     * @throws Exception
     *
     * @Author 이혁수
     */
    @RequestMapping(value = "/consumer/shutdown")
    @ResponseBody
    public String shutdownBuffer(@RequestParam(value = "topicName", required = true, defaultValue = "") String topicName) throws Exception {
        if (consumerDataService.checkTopic(topicName)) {
            statusService.deleteBufferStatus(topicName);
            consumerDataService.consumerShutdown(topicName);
            consumerDataService.deleteData(topicName);
            return "Request - shutdown consumer : " + topicName;
        } else {
            return String.format("Check the topic name");
        }
    }
}
