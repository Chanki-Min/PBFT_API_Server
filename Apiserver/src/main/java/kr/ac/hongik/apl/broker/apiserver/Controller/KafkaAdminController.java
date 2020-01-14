package kr.ac.hongik.apl.broker.apiserver.Controller;


import org.apache.commons.lang3.NotImplementedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController(value = "/kafka")
public class KafkaAdminController {
	/*
	* Kafka의 토픽 생성, 삭제 관리를 처리하는 KafkaAdmin 객체를 오토와이어 해줍니다
	* @AutoWired
	* KafkaAdmin kafkaAdmin;
	 */

	@RequestMapping(value = "/topic/describe")
	public String describeTopicRequest() {
		/*
		* 권한 인증이 필요하다면 인증 후에 현재 카프카 클러스터의 토픽 정보를 반환한다
		*/
		throw new NotImplementedException("구현합시다");
	}

	@RequestMapping(value = "/topic/create")
	public String createTopicRequest() {
		/*
		* 토픽 이름, partition, replication factor, configs, replicaId등을 받아서 토픽을 생성하고, 토픽의 정보를 반환한다
		*
		* 만약 각 블록체인의 단위가 토픽이라면 pbft에 새로운 블록체인을 만들어줄 필요가 있다 (또는 consumer가 삽입시에 생성할수도 있을듯, 논의 필요)
		*
		* ref) https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/admin/AdminClient.html
		* ref) https://docs.spring.io/spring-kafka/docs/2.4.0.RELEASE/reference/html/#configuring-topics
		*/
		throw new NotImplementedException("구현합시다");
	}

}
