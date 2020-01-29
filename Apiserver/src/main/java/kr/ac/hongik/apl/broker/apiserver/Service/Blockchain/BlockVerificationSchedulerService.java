package kr.ac.hongik.apl.broker.apiserver.Service.Blockchain;

import kr.ac.hongik.apl.broker.apiserver.Configuration.SchedulerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.util.ConcurrentModificationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BlockChainVerifier의 verifyBlockChain()메서드를 task로 삼아 ThreadPoolTaskScheduler에 crontab을 스캐쥴링합니다
 */
@Slf4j
@Service
@PropertySource("classpath:broker.properties")
public class BlockVerificationSchedulerService implements InitializingBean, DisposableBean {
	private static final String CONCURRENT_STAT_SCHEDULER_ERROR = "BlockVerificationSchedulerService is already running";
	private static final String CONCURRENT_CRON_SCHEDULER_ERROR = "BlockVerificationSchedulerService's cronTap is already modifying";

	private final Environment env;
	private final BlockChainVerifier blockChainVerifier;
	private final ThreadPoolTaskScheduler taskScheduler;

	/*
	* Bean의 Thread-safety를 보장하기 위한 Atomic boolean var
	 */
	private AtomicBoolean isSchedulerRunning = new AtomicBoolean(false);
	private AtomicBoolean isFutureTaskModifying = new AtomicBoolean(false);

	private ScheduledFuture<?> futureTask = null;
	private String cron = null;

	@Autowired
	public BlockVerificationSchedulerService(Environment env, BlockChainVerifier blockChainVerifier, ThreadPoolTaskScheduler taskScheduler) {
		this.env = env;
		this.blockChainVerifier = blockChainVerifier;
		this.taskScheduler = taskScheduler;
	}

	public void start() {
		if(isSchedulerRunning.compareAndSet(false, true)) {
			futureTask = taskScheduler.schedule(blockChainVerifier::verifyBlockChain, new CronTrigger(cron));
		} else{
			log.error(CONCURRENT_STAT_SCHEDULER_ERROR);
			throw new ConcurrentModificationException(CONCURRENT_STAT_SCHEDULER_ERROR);
		}
	}

	public void changeCronSchedule(String cron) {
		if(isFutureTaskModifying.compareAndSet(false, true)) {
			isSchedulerRunning.set(false);
			if (futureTask != null && !futureTask.isCancelled()) {
				futureTask.cancel(true);
			}
			this.cron = cron;
			this.start();
			isFutureTaskModifying.set(false);
		} else {
			log.error(CONCURRENT_CRON_SCHEDULER_ERROR);
			throw new ConcurrentModificationException(CONCURRENT_CRON_SCHEDULER_ERROR);
		}
	}

	public void cancelVerificationSchedule() {
		if(isFutureTaskModifying.compareAndSet(false, true)) {
			if (futureTask != null && !futureTask.isCancelled()) {
				futureTask.cancel(true);
			}
			isFutureTaskModifying.set(false);
		} else {
			log.error(CONCURRENT_CRON_SCHEDULER_ERROR);
			throw new ConcurrentModificationException(CONCURRENT_CRON_SCHEDULER_ERROR);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if( Boolean.parseBoolean(env.getProperty(SchedulerConfiguration.SCHEDULER_START_WHEN_INITIALIZE))) {
			isSchedulerRunning.set(true);
			cron = env.getProperty(SchedulerConfiguration.SCHEDULER_DEFAULT_CRON);
			this.start();
		}
	}

	@Override
	public void destroy() throws Exception {
		isSchedulerRunning.set(false);
		taskScheduler.destroy();
	}
}
