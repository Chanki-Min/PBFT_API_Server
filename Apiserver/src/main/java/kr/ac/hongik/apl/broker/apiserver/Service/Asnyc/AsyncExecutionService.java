package kr.ac.hongik.apl.broker.apiserver.Service.Asnyc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * 이 서비스는 Spring의 @Async 어노테이션의 작동 방식의 한계를 보완하기 위하여 사용된다.
 * Spring에서 @Async가 붙은 메소드를 실행하면 어노테이션에 지정된 스레드 풀을 사용하여 비동기적으로 메소드가 실행이 되지만,
 * 이는 Spring이 객체가 다른 객체의 메소드를 불러오는 과정을 reflection으로 하이재킹하여 쓰레드 풀에 할당하는 것이기 때문에 this의 메소드는
 * 메소드에 @Async 어노테이션이 붙어있어도 비동기적으로 실행되지 않는다.
 * <p>
 * 따라서 다른 객체의 Runnable interface를 Async하게 실행시키는 run메소드를 정의하여 사용한다.
 */
@Service
@Slf4j
public class AsyncExecutionService {

    @Async(value = "consumerThreadPool")
    public CompletableFuture<Void> runAsConsumerExecutor(Supplier<Exception> function) {
        CompletableFuture<Exception> future = new CompletableFuture<>();
        future.complete(function.get());
        return future.thenAcceptAsync(exceptionOrNull -> restartOrStopConsumer(function, exceptionOrNull));
    }

    /**
     * 이 메소드는 컨슈머가 어떠한 이유로든 종료되었을 때 사용되는 메소드이다.
     * 만약 에러로 인해 컨슈머가 종료 되었다면, exceptionOrNull 이 null 아닐 것이고, 에러 로그를 기록 후 컨슈머를 재가동시킨다.
     * 만약 정상적인 종료를 원하는 것이라면, 재가동하지 않고 메소드를 완료한다.
     *
     * @param function 컨슈머 시작하는 함수 인터페이스
     * @param exceptionOrNull 컨슈머가 종료되었을 때 반환되는 예외처리 변수. null 이 아니면 예기치 못한 에러이며 null 일 시 정상 종료를 뜻한다.
     *
     * @Author 이혁수
     */
    public void restartOrStopConsumer(Supplier<Exception> function, Exception exceptionOrNull) {
        if (exceptionOrNull != null) {
            log.error("there is a error exception",exceptionOrNull);
            log.info(String.format("recover from error successfully!"));
            runAsConsumerExecutor(function);
        } else {
            log.info(String.format("shutdown consumer successfully"));
        }
    }

    @Async(value = "searchResultVerifierThreadPool")
    public void runAsSearchResultVerifierExecutor(Runnable runnable) {
        runnable.run();
    }
}
