package kr.ac.hongik.apl.broker.apiserver.TestConfiguration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@TestConfiguration
@PropertySource("classpath:testKafka.properties")
public class TestKafkaConsumerConfiguration {
	public static final String BUFFERED_CONSUMER_TOPICS = "kafka.listener.service.topic";
	public static final String BUFFERED_CONSUMER_MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
	public static final String BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.isHashListInclude";
	public static final String BUFFERED_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.timeout.millis";
	public static final String BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.poll.interval.millis";

	public static final String IMMEDIATE_CONSUMER_TOPICS = "kafka.listener.service.immediate.topic";
	public static final String IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.immediate.isHashListInclude";
	public static final String IMMEDIATE_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.immediate.timeout.millis";
	public static final String IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.immediate.poll.interval.millis";

	@Autowired
	Environment env;

	@Bean(name = "consumerConfigs")
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "Lee");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		return props;
	}

	@Bean(name = "bufferedClientConfigs")
	public Map<String, Object> bufferClientConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(BUFFERED_CONSUMER_TOPICS, Arrays.asList(env.getProperty(BUFFERED_CONSUMER_TOPICS)));
		props.put(BUFFERED_CONSUMER_MIN_BATCH_SIZE, Integer.parseInt(env.getProperty(BUFFERED_CONSUMER_MIN_BATCH_SIZE)));
		props.put(BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE, Boolean.parseBoolean(env.getProperty(BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE)));
		props.put(BUFFERED_CONSUMER_TIMEOUT_MILLIS, Integer.parseInt(env.getProperty(BUFFERED_CONSUMER_TIMEOUT_MILLIS)));
		props.put(BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS, Long.parseLong(env.getProperty(BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS)));
		return props;
	}
	@Bean(name = "ImmediateClientConfigs")
	public Map<String, Object> ImmediateServiceConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(IMMEDIATE_CONSUMER_TOPICS, Arrays.asList(env.getProperty(IMMEDIATE_CONSUMER_TOPICS)));
		props.put(IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE, Boolean.parseBoolean(env.getProperty(IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE)));
		props.put(IMMEDIATE_CONSUMER_TIMEOUT_MILLIS, Integer.parseInt(env.getProperty(IMMEDIATE_CONSUMER_TIMEOUT_MILLIS)));
		props.put(IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS, Long.parseLong(env.getProperty(IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS)));
		return props;
	}
}
