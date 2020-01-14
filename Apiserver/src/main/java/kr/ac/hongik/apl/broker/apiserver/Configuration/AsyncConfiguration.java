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
public class AsyncConfiguration {
	public static final String CORE_POOL_SIZE = "broker.async.corePoolSize";
	public static final String MAX_POOL_SIZE = "broker.async.maxPoolSize";
	public static final String QUEUE_CAPACITY = "broker.async.queueCapacity";
	public static final String THREAD_NAME_PREFIX = "broker.async.threadNamePrefix";

	@Autowired
	Environment env;

	/**
	 * @return initialized ThreadPoolTaskExecutor
	 */
	@Bean(name = "threadPoolTaskExecutor")
	public Executor threadPoolTaskExecutor() {
		/**
		 * 스레드 풀을 추상화한 Executor Bean을 반환합니다
		 * CorePoolSize : 최소한 유지해야 할 스레드 갯수
		 * MaxPoolSize : 최대 사용 가능한 스레드 갯수
		 * QueueCapacity : 작업(runnable)을 저장할 블로킹 큐의 크기 (기본은 LinkedBlockingQueue 이므로 크기 제한은 없다)
		 * ThreadNamePrefix : 생성된 스레드의 prefix(접두어)를 정의한다
		 */
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(CORE_POOL_SIZE))));
		taskExecutor.setMaxPoolSize(Integer.parseInt(Objects.requireNonNull(env.getProperty(MAX_POOL_SIZE))));
		taskExecutor.setQueueCapacity(Integer.parseInt(Objects.requireNonNull(env.getProperty(QUEUE_CAPACITY))));
		taskExecutor.setThreadNamePrefix(env.getProperty(THREAD_NAME_PREFIX));
		taskExecutor.initialize();
		return taskExecutor;
	}
}
