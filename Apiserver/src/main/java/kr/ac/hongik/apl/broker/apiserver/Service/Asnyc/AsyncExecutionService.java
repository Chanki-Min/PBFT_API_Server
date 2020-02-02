package kr.ac.hongik.apl.broker.apiserver.Service.Asnyc;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerInfo;
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
// TODO : 출력문들 로그로 바꾸기
@Service
public class AsyncExecutionService {

    @Async(value = "consumerThreadPool")
    public CompletableFuture<Void> runAsConsumerExecutor(Supplier<ConsumerInfo> function, ConsumerInfo consumerInfo) {
        CompletableFuture<ConsumerInfo> future = new CompletableFuture<>();
        future.complete(function.get());
        return future.thenAcceptAsync(consumer -> restartOrStopConsumer(function, consumer));
    }

    /**
     * 이 메소드는 컨슈머가 어떠한 이유로든 종료되었을 때 사용되는 메소드이다.
     * 만약 에러로 인해 컨슈머가 종료 되었다면, 에러 로그를 기록 후 반환된 컨슈머 정보를 통해 컨슈머를 재가동시킨다.
     * 만약 컨슈머의 설정 정보를 동적으로 바꾸기 위해 종료를 하는 경우라면,
     * 컨슈머 종료시 반환 되는 객체에 이미 사용자가 바꾸길 원하는 정보가 쓰여져 반환되기 때문에
     * 정보 변경 상황을 로그로 기록해 주고 그 객체를 다시 넘겨 재시동 한다.
     * 만약 정상적인 종료를 원하는 것이라면, 재가동하지 않고 메소드를 완료한다.
     *
     * @param function     컨슈머 시작하는 함수 인터페이스
     * @param consumerInfo 컨슈머가 종료되었을 때 반환되는 컨슈머 정보 객체이자 컨슈머 재가동 시 사용될 객체
     */
    public void restartOrStopConsumer(Supplier<ConsumerInfo> function, ConsumerInfo consumerInfo) {
        if (consumerInfo.isError()) {
            // TODO : 에러에서 반환한 exception 로그 기록
            consumerInfo.setError(false);
            System.out.println("recover from error successfully");
            runAsConsumerExecutor(function, consumerInfo);
        } else if (consumerInfo.isSettings()) {
            consumerInfo.setSettings(false);
            System.out.println(consumerInfo.getTopicName());
            System.out.println(consumerInfo.getMinBatchSize());
            System.out.println(consumerInfo.getTimeout());
            System.out.println("change settings and restart consumer successfully");
            runAsConsumerExecutor(function, consumerInfo);
        } else {
            consumerInfo.setShutdown(false);
            System.out.println(consumerInfo.getTopicName() + "'s consumer shut down successfully");
        }
    }

    @Async(value = "searchResultVerifierThreadPool")
    public void runAsSearchResultVerifierExecutor(Runnable runnable) {
        runnable.run();
    }
}