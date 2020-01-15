package kr.ac.hongik.apl.broker.apiserver.Configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@PropertySource("classpath:broker.properties")
public class SchedulerConfiguration {
	public static final String SCHEDULER_POOL_SIZE = "broker.scheduler.pool.size";
	public static final String SCHEDULER_THREAD_NAME_PREFIX = "broker.scheduler.thread.name.prefix";
	public static final String SCHEDULER_DEFAULT_CRON = "broker.scheduler.default.cron";

	@Autowired
	Environment env;

	/**
	 * 지정한 작업을 지정한 스케쥴대로 주기적으로 실행하는 TheadPoolTaskScheduler를 생성합니다.
	 *
	 * @return initialize 된 TheadPoolTaskScheduler
	 */
	@Bean
	public ThreadPoolTaskScheduler schedulerExecutor() {
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.setPoolSize(Integer.parseInt(env.getProperty(SCHEDULER_POOL_SIZE)));
		taskScheduler.setThreadNamePrefix(env.getProperty(SCHEDULER_THREAD_NAME_PREFIX));
		taskScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

		return taskScheduler;
	}
}
