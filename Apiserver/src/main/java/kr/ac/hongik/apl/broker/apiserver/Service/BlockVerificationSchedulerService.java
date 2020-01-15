package kr.ac.hongik.apl.broker.apiserver.Service;

import kr.ac.hongik.apl.broker.apiserver.Configuration.SchedulerConfiguration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledFuture;

/**
 * BlockChainVerifier의 verifyBlockChain()메서드를 task로 삼아 ThreadPoolTaskScheduler에 crontab을 스캐쥴링합니다
 */
@Service
@PropertySource("classpath:broker.properties")
public class BlockVerificationSchedulerService implements InitializingBean {
	private final Environment env;
	private final BlockChainVerifier blockChainVerifier;
	private final ThreadPoolTaskScheduler taskScheduler;

	//TODO : thread-safty 보장하기
	private ScheduledFuture<?> futureTask = null;
	private String cron = null;

	@Autowired
	public BlockVerificationSchedulerService(Environment env, BlockChainVerifier blockChainVerifier, ThreadPoolTaskScheduler taskScheduler) {
		this.env = env;
		this.blockChainVerifier = blockChainVerifier;
		this.taskScheduler = taskScheduler;
	}

	public void start() {
		futureTask = taskScheduler.schedule(blockChainVerifier::verifyBlockChain, new CronTrigger(cron));
	}

	public void changeCronSchedule(String cron) {
		if(futureTask != null) {
			futureTask.cancel(true);
		}
		this.cron = cron;
		this.start();
	}

	public void stopVerification() {
		futureTask.cancel(true);
		taskScheduler.shutdown();
	}


	@Override
	public void afterPropertiesSet() throws Exception {
		cron = env.getProperty(SchedulerConfiguration.SCHEDULER_DEFAULT_CRON);
		//TODO : 검증 스케쥴러 활성화 (디버깅을 위하여 주석처리함)
		//this.start();
	}
}
