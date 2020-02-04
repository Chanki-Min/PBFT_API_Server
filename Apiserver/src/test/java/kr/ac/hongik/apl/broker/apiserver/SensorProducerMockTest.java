package kr.ac.hongik.apl.broker.apiserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.TestConfiguration.TestKafkaProducerConfiguration;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Slf4j
@SpringBootTest(
		properties = {
			"producer.topic=chanki-buffered",
			"sensor.dummyData={\"accel_X\":\"31\",\"accel_Y\":\"29\",\"accel_Z\":\"1037\",\"mag_X\":\"7\",\"mag_Y\":\"10\",\"mag_Z\":\"-43\",\"mag_R\":\"6476\",\"gyro_X\":\"6347\",\"gyro_Y\":\"3662\",\"gyro_Z\":\"4516\",\"rh\":\"52\",\"pressure\":\"100388\",\"light\":\"446400\",\"noise\":\"0.00229065\",\"temp\":\"21591\"}"
		},
		classes = {
				TestKafkaProducerConfiguration.class, ObjectMapper.class
		}
)
class SensorProducerMockTest {

	@Value("${producer.topic}")
	private String producingTopic;
	@Value("${sensor.dummyData}")
	private String dummyData;

	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Test
	void contextLoads() {
	}

	@Test
	void sensorDataDeserializationTest() {
		Map<String, String> sensorData = Assertions.assertDoesNotThrow(this::genDummySensorDataAsMap);
		sensorData.forEach((key, value) -> System.err.printf("%s : %s \n", key, value));
	}

	@Test
	void sendDummySensorData() throws InterruptedException {
		Runnable producer = () -> {
			while(true) {
				Map<String, String> data = genDummySensorDataAsMap();
				kafkaTemplate.send(producingTopic, data);
				log.debug(String.format("producer sent data to kafka. data : %s", data.toString()));
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		};

		Thread producingThread = new Thread(producer);
		producingThread.start();
		producingThread.join();
	}


	private Map<String, String> genDummySensorDataAsMap() {
		try {
			SensorData data =  objectMapper.readValue(dummyData, SensorData.class);
			Map<String, String> mappedData = objectMapper.convertValue(data, new TypeReference<>() {});
			return mappedData;
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
class SensorData {
	private String accel_X;
	private String accel_Y;
	private String accel_Z;
	private String mag_X;
	private String mag_Y;
	private String mag_Z;
	private String mag_R;
	private String gyro_X;
	private String gyro_Y;
	private String gyro_Z;
	private String rh;
	private String pressure;
	private String light;
	private String noise;
	private String temp;
}