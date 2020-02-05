package kr.ac.hongik.apl.broker.apiserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.TestConfiguration.TestKafkaConsumerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

@SpringBootTest(classes = {ObjectMapper.class, TestKafkaConsumerConfiguration.class})
public class SensorConsumerTest {
	public static final String BUFFERED_CONSUMER_TOPICS = "kafka.listener.service.topic";
	public static final String BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.poll.interval.millis";

	@Resource(name = "consumerConfigs")
	private Map<String, Object> consumerConfigs;
	@Resource(name = "bufferedClientConfigs")
	private Map<String, Object> bufferedClientConfigs;

	@Test
	void bufferedConsumeTest() {
		KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerConfigs);
		consumer.subscribe((Collection<String>) bufferedClientConfigs.get(BUFFERED_CONSUMER_TOPICS));

		while(true) {
			ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis((Long) bufferedClientConfigs.get(BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS)));
			for(TopicPartition topicPartition : records.partitions()) {
				for(ConsumerRecord<String, Object> record : records.records(topicPartition)) {
					Map<String, String> data = (Map<String, String>) record.value();
					data.entrySet().stream()
							.forEach(entry -> System.out.printf("%s : %s \n", entry.getKey(), entry.getValue()));
				}
			}
		}

	}
}
