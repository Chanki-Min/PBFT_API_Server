package kr.ac.hongik.apl.broker.apiserver.Configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Objects;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@PropertySource("classpath:broker.properties")
public class ThreadPoolTaskExecutorConfiguration {
	public static final String CONSUMER_THREAD_CORE_POOL_SIZE = "broker.consumer.thread.corePoolSize";
	public static final String CONSUMER_THREAD_MAX_POOL_SIZE = "broker.consumer.thread.maxPoolSize";
	public static final String CONSUMER_THREAD_QUEUE_CAPACITY = "broker.consumer.thread.queueCapacity";
	public static final String CONSUMER_THREAD_THREAD_NAME_PREFIX = "broker.consumer.thread.threadNamePrefix";

	public static final String SEARCH_RESULT_VERIFIER_THREAD_CORE_POOL_SIZE = "broker.searchResultVerifier.thread.corePoolSize";
	public static final String SEARCH_RESULT_VERIFIER_THREAD_MAX_POOL_SIZE = "broker.searchResultVerifier.thread.maxPoolSize";
	public static final String SEARCH_RESULT_VERIFIER_THREAD_QUEUE_CAPACITY = "broker.searchResultVerifier.thread.queueCapacity";
	public static final String SEARCH_RESULT_VERIFIER_THREAD_NAME_PREFIX ="broker.searchResultVerifier.thread.threadNamePrefix";

	@Autowired
	Environment env;

	/**
	 * Kafka 컨슈머를 쓰레드를 관리하는 풀을 생성한다
	 * @return initialized ThreadPoolTaskExecutor
	 */
	@Bean(name = "consumerThreadPool")
	public Executor consumerThreadPool() {
		/**
		 * 컨슈머들을 비동기적으로 실행시키는 스레드 풀을 추상화한 Executor Bean을 반환합니다
		 * CorePoolSize : 최소한 유지해야 할 스레드 갯수
		 * MaxPoolSize : 최대 사용 가능한 스레드 갯수
		 * QueueCapacity : 작업(runnable)을 저장할 블로킹 큐의 크기 (기본은 LinkedBlockingQueue이다)
		 *
		 *  이 ThreadPoolTaskExecutor는 다음의 규칙으로 실행된다
		 *  1.core size보다 적은 스레드가 있을 경우 남은 core size만큼의 스레드는 대기한다
		 *  2.core szie를 초과하는 스레드가 있을 경우 이 객체는 maxPoolSize만큼까지 스레드를 추가하는 것을 선호한다
		 *  3.maxPoolSize를 초과하는 스레드는 Queue에 저장된다
		 *  4.최대 용량 (max+queue)를 초과하는 스레드는 reject된다
		 *
		 * ThreadNamePrefix : 생성된 스레드의 prefix(접두어)를 정의한다
		 */
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(CONSUMER_THREAD_CORE_POOL_SIZE))));
		taskExecutor.setMaxPoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(CONSUMER_THREAD_MAX_POOL_SIZE))));
		taskExecutor.setQueueCapacity(
				env.getProperty(CONSUMER_THREAD_QUEUE_CAPACITY) == null ? Integer.MAX_VALUE : Integer.parseInt(Objects.requireNonNull(env.getProperty(CONSUMER_THREAD_QUEUE_CAPACITY)))
		);
		taskExecutor.setThreadNamePrefix(env.getProperty(CONSUMER_THREAD_THREAD_NAME_PREFIX));
		taskExecutor.initialize();
		return taskExecutor;
	}

	/**
	 * 검색 결과를 검증하는 Client 쓰레드를 관리하는 풀을 생성한다
	 *
	 *  이 ThreadPoolTaskExecutor는 다음의 규칙으로 실행된다
	 *  1.core size보다 적은 스레드가 있을 경우 남은 core size만큼의 스레드는 대기한다
	 *  2.core szie를 초과하는 스레드가 있을 경우 이 객체는 maxPoolSize만큼까지 스레드를 추가하는 것을 선호한다
	 *  3.maxPoolSize를 초과하는 스레드는 Queue에 저장된다
	 *  4.최대 용량 (max+queue)를 초과하는 스레드는 reject된다
	 *
	 * @return initialized ThreadPoolTaskExecutor
	 */
	@Bean(name = "searchResultVerifierThreadPool")
	public Executor searchResultVerifierThreadPool() {
		/**
		 * 검색 결과 검증 작업을 비동기적으로 실행시키는 스레드 풀을 추상화한 Executor Bean을 반환합니다
		 * CorePoolSize : 최소한 유지해야 할 스레드 갯수
		 * MaxPoolSize : 최대 사용 가능한 스레드 갯수
		 * QueueCapacity : 작업(runnable)을 저장할 블로킹 큐의 크기 (기본은 LinkedBlockingQueue 이므로 크기 제한은 없다)
		 * ThreadNamePrefix : 생성된 스레드의 prefix(접두어)를 정의한다
		 */
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(SEARCH_RESULT_VERIFIER_THREAD_CORE_POOL_SIZE))));
		taskExecutor.setMaxPoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(SEARCH_RESULT_VERIFIER_THREAD_MAX_POOL_SIZE))));

		taskExecutor.setQueueCapacity(
				env.getProperty(SEARCH_RESULT_VERIFIER_THREAD_QUEUE_CAPACITY) == null ? Integer.MAX_VALUE : Integer.parseInt(Objects.requireNonNull(env.getProperty(SEARCH_RESULT_VERIFIER_THREAD_QUEUE_CAPACITY)))
		);
		taskExecutor.setThreadNamePrefix(env.getProperty(SEARCH_RESULT_VERIFIER_THREAD_NAME_PREFIX));
		taskExecutor.initialize();
		return taskExecutor;
	}
}
