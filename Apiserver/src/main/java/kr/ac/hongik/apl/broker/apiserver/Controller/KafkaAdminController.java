package kr.ac.hongik.apl.broker.apiserver.Controller;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.*;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerFactoryService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

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
	KafkaAdmin kafkaAdmin;

	ObjectMapper objectMapper;

	ConsumerFactoryService consumerFactoryService;

	AsyncExecutionService asyncExecutionService;

	StatusService statusService;

	@Autowired
	public KafkaAdminController(KafkaAdmin kafkaAdmin, ObjectMapper objectMapper, ConsumerFactoryService consumerFactoryService, AsyncExecutionService asyncExecutionService, StatusService statusService) {
		this.kafkaAdmin = kafkaAdmin;
		this.objectMapper = objectMapper;
		this.consumerFactoryService = consumerFactoryService;
		this.asyncExecutionService = asyncExecutionService;
		this.statusService = statusService;
	}

	@PostMapping("/topic/create/buf")
	@ResponseBody
	public String createBuffConsumer(@RequestBody ConsumerBufferConfigs configs) throws JsonProcessingException {
		BufferedConsumingPbftClient bufferedConsumingPbftClient =
				consumerFactoryService.MakeBufferedConsumer(configs.getCommonConfigs(),configs.getBuffConfigs());
		statusService.addBufferStatus(configs);
//TODO: NULL 지우고 혁수형과 의논
		asyncExecutionService.runAsConsumerExecutor(bufferedConsumingPbftClient::startConsumer, null);
		return String.format("New Buffer Consumer added!");
	}

	@PostMapping("/topic/create/imme")
	@ResponseBody
	public String createImmediateConsumer(@RequestBody ConsumerImmediateConfigs configs) throws JsonProcessingException {
		ImmediateConsumingPbftClient immediateConsumingPbftClient =
				consumerFactoryService.MakeImmediateConsumer(configs.getCommonConfigs(),configs.getConImmeConfigs());
		statusService.addImmediateStatus(configs);
		asyncExecutionService.runAsConsumerExecutor(immediateConsumingPbftClient::startConsumer, null);
		return String.format("New Immediate Consumer added!");
	}


	//TODO : 권한 인증
	@PostMapping(value = "/topic/describe")
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

			for(Map.Entry<String, KafkaFuture<TopicDescription>> entry : TopicResult.values().entrySet()) {
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

}
