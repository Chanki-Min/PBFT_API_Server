package kr.ac.hongik.apl.broker.apiserver.Service;

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 동적 (런타임) 스케쥴링을 지원하는 추상 스케쥴러 클래스이다
 */
public abstract class AbstractDynamicTaskScheduler {
	private ThreadPoolTaskScheduler scheduler;

	public void startScheduler() {
		scheduler = new ThreadPoolTaskScheduler();
		scheduler.initialize();
		scheduler.schedule(getRunnable(), getTrigger());
	}

	public void stopScheduler() {
		scheduler.destroy();
	}

	private Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				task();
			}
		};
	}

	/**
	 * 이 추상 메소드에 실행할 작업을 정의한다
	 */
	public abstract void task();

	/**
	 * 이 추상 메소드에 실행 주기를 정의한다
	 * @return 트리거
	 */
	public abstract Trigger getTrigger();

}
