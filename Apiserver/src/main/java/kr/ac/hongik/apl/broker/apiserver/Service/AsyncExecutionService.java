package kr.ac.hongik.apl.broker.apiserver.Service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * 이 서비스는 Spring의 @Async 어노테이션의 작동 방식의 한계를 보완하기 위하여 사용된다.
 * Spring에서 @Async가 붙은 메소드를 실행하면 어노테이션에 지정된 스레드 풀을 사용하여 비동기적으로 메소드가 실행이 되지만,
 * 이는 Spring이 객체가 다른 객체의 메소드를 불러오는 과정을 reflection으로 하이재킹하여 쓰레드 풀에 할당하는 것이기 때문에 this의 메소드는
 * 메소드에 @Async 어노테이션이 붙어있어도 비동기적으로 실행되지 않는다.
 *
 * 따라서 다른 객체의 Runnable interface를 Async하게 실행시키는 run메소드를 정의하여 사용한다.
 */
@Service
public class AsyncExecutionService {
	@Async(value = "consumerThreadPool")
	public void runAsConsumerExecutor(Runnable runnable) {
		runnable.run();
	}

	@Async(value = "searchResultVerifierThreadPool")
	public void runAsSearchResultVerifierExecutor(Runnable runnable) {
		runnable.run();
	}
}