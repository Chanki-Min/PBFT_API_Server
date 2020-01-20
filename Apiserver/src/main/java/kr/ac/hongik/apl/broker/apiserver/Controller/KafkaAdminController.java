package kr.ac.hongik.apl.broker.apiserver.Controller;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.TopicData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Controller
@RequestMapping("/kafka")
public class KafkaAdminController {
	/*
	* Kafka의 토픽 생성, 삭제 관리를 처리하는 KafkaAdmin 객체를 오토와이어 해줍니다
	 */
	@Autowired
	KafkaAdmin kafkaAdmin;

	@Autowired
	ObjectMapper objectMapper;


	@PostMapping("/topic/create")
	@ResponseBody
	public String createTopic(@RequestParam String topic, @RequestParam String numpartitions, @RequestParam String replicas) {

		log.info("Creating topic '{}'", topic);
		AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig());

		CreateTopicsResult result = adminClient.createTopics(Collections.singleton(getTopic(topic, Integer.parseInt(numpartitions), Short.valueOf(replicas))));
		log.info(result.toString());

		log.info("Describing topic '{}'", topic); //생성한 토픽 정보 출력

		DescribeTopicsResult TopicResult = adminClient.describeTopics(Collections.singletonList(topic));
		TopicData topicData = new TopicData();
		AtomicReference<String> mappedValue = new AtomicReference<>(new String());

		TopicResult.values().forEach((key, value) -> {
			try {
				topicData.setName(value.get().name());
				topicData.setInternal(value.get().isInternal());
				topicData.setPartitions(value.get().partitions());

				objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
				mappedValue.set(objectMapper.writeValueAsString(topicData));

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			System.out.println(mappedValue);
		});
		return mappedValue.get();

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
		AtomicReference<String> mappedValue = new AtomicReference<>(new String());

		try {
			ListTopicsResult list = adminClient.listTopics();

			List topics = list.names().get().stream()
					.collect(Collectors.toList());

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

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		return mappedValue.get();
	}




	private NewTopic getTopic(String topic, int numpartitions, short replicas) {
		return new NewTopic(topic, numpartitions, replicas);
	}

}
