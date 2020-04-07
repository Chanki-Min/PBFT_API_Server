package kr.ac.hongik.apl.broker.apiserver.Controller;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.*;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerFactoryService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.TopicAndConsumerCreationService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Controller
@RequestMapping("/kafka")
public class KafkaAdminController {
    private final ObjectMapper objectMapper;
    private final ConsumerFactoryService consumerFactoryService;
    private final AsyncExecutionService asyncExecutionService;
    private final ConsumerDataService consumerDataService;
	private final StatusService statusService;
	private final TopicAndConsumerCreationService topicAndConsumerCreationService;

	@Autowired
    public KafkaAdminController(ObjectMapper objectMapper, ConsumerFactoryService consumerFactoryService, AsyncExecutionService asyncExecutionService, ConsumerDataService consumerDataService, StatusService statusService, TopicAndConsumerCreationService topicAndConsumerCreationService) {
        this.objectMapper = objectMapper;
        this.consumerFactoryService = consumerFactoryService;
        this.asyncExecutionService = asyncExecutionService;
        this.consumerDataService = consumerDataService;
        this.statusService = statusService;
        this.topicAndConsumerCreationService = topicAndConsumerCreationService;
    }

    /**
     * @param info topic과 bufferConsumer를 생성하기 위한 정보가 담긴 객체
     *
     * @author 이지민
     */
    @PostMapping("/create/buffer")
    @ResponseBody
    public String create(@RequestBody TopicBufferConsumerInfo info) throws ExecutionException, InterruptedException {
        topicAndConsumerCreationService.createTopic(info.toNewTopic(), info.toAdminConfigs());
        topicAndConsumerCreationService.createBufferConsumer(info.toBufferConfigs());
        return info.toString();
	}

	// TODO : /consumer 추가 하기.

    @PostMapping("/create/immediate")
    @ResponseBody
    public String create(@RequestBody TopicImmediateConsumerInfo info) throws ExecutionException, InterruptedException {
        topicAndConsumerCreationService.createTopic(info.toNewTopic(), info.toAdminConfigs());
        topicAndConsumerCreationService.createImmediateConsumer(info.toImmediateConfigs());
        return info.toString();
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
     * @param info 클라이언트가 희망하는 컨슈머 config 가 담긴 객체. 이미 실행중인 컨슈머와 같은 토픽을 구독하여야 한다.
     * @return
     * @throws Exception
     *
     * @author 이혁수
     */
    @PostMapping("/consumer/change/buffer")
    @ResponseBody
    public String changeBuffConsumer(@RequestBody TopicBufferConsumerInfo info) throws Exception {
            String topicName = info.getTopic();
            if (consumerDataService.checkTopic(topicName)) {
                statusService.deleteBufferStatus(topicName);
                consumerDataService.consumerShutdown(topicName);
                consumerDataService.deleteData(topicName);
                topicAndConsumerCreationService.createBufferConsumer(info.toBufferConfigs());
                log.info("Buffer Consumer has been changed");
                return String.format("Buffer Consumer has been changed!");
            } else {
                return String.format("Check the topic name");
            }
    }

    /**
     * 현재 topic들의 정보 리스트를 출력한다.
     * @param map bootstrap-server주소가 담긴 map  ex) {"configs":["223.194.70.105:19590","223.194.70.105:19690","223.194.70.105:19790"]}
     * @return
     *
     * @author 이지민
     */
    //TODO : 권한 인증
    @PostMapping(value = "/consumer/describe")
    @ResponseBody
    public String describeTopicRequest(@RequestBody Map<String, Object> map) {
        /*
         * 권한 인증이 필요하다면 인증 후에 현재 카프카 클러스터의 토픽 정보를 반환한다
         */

        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, map.get("configs"));

        log.info("Listing topic----------------------");

        AdminClient adminClient = AdminClient.create(props);
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
     * @author 이혁수
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
