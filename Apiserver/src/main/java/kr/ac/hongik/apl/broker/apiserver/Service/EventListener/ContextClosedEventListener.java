package kr.ac.hongik.apl.broker.apiserver.Service.EventListener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static kr.ac.hongik.apl.broker.apiserver.Configuration.ContextClosedEventListenerConfiguration.*;

/**
 * Spring이 shutdown되어 singleton bean들이 소멸되기 직전의 상태에서 발생되는 ContextClosedEvent를 잡아서 gracefully shutdown 을 실행하는
 * 메소드가 정의된 클래스이다. 또한 TomcatConnectorCustomizer를 구현하여 Tomcat도 안전하게 종료할 수 있도록 한다.
 *
 *
 *
 * @author Chanki_Min
 */
@Slf4j
@Component
public class ContextClosedEventListener implements TomcatConnectorCustomizer {
	@Resource(name = "contextClosedEventListenerConfig")
	private Map<String, Object> contextClosedEventListenerConfig;

	private final ConsumerDataService consumerDataService;
	private final ObjectMapper objectMapper;
	private volatile Connector connector;

	@Autowired
	public ContextClosedEventListener(ConsumerDataService consumerDataService, ObjectMapper objectMapper) {
		this.consumerDataService = consumerDataService;
		this.objectMapper = objectMapper;
	}

	@Autowired
	@Qualifier(value = "executeThreadPool")
	private Executor executeThreadPool;

	/**
	 * TomcatConnectorCustomizer의 요구 메소드. tomcat의 connector를 반환한다. 이것을 TomcatCustomizerConfiguration이 이용한다.
	 *
	 * @param connector tomcat의 connector
	 */
	@Override
	public void customize(Connector connector) {
		this.connector = connector;
	}

	/**
	 * Spring 종료시 ContextClosedEvent를 인자로 받아 호출되는 메소드이다. 이 메소드는 아래 순서대로 shutdown을 진행한다.
	 * 1. Tomcat 웹서버가 더 이상의 request를 받지 않도록 만든다.
	 * 2. Tomcat 웹서버의 thread pool을 정지시키고, 남은 Job이 끝날 때까지 ContextClosedEvent만큼 대기한다.
	 * 3. Tomcat이 ContextClosedEvent만큼 대기해도 끝나지 않는다면 강제로 Job을 interrupt한다.
	 * 4. ConsumerDataService에서 모든 consumer 객체를 순회하며 destroy하고. 이후 정해진 시간만큼 대기한다.기
	 * 4. TODO : execution thread pool 이 더 이상의 작업을 받지 않도록 하고, 모든 작업이 끝날 때까지 대기한다.
	 *
	 * @param event ContextClosedEvent를 인자로 받아 호출되기 위하여 받는 인자. 사용하지 않는다.
	 */
	@EventListener
	public void shutdownGracefully(ContextClosedEvent event) {
		int tomcatTerminationTimeoutMillis = (int) contextClosedEventListenerConfig.get(TOMCAT_TERMINATION_TIMEOUT_MILLIS);
		int executeThreadAwaitTime =  (int) contextClosedEventListenerConfig.get(EXECUTE_THREAD_AWIATTIME);
		int consumerThreadAwaitTime = (int) contextClosedEventListenerConfig.get(CONSUMER_THREAD_AWIATTIME);

		log.info(String.format("Got ContextClosedEvent. try to shutdown server gracefully..."));
		log.info("Shutting down tomcat web server...");
		//Tomcat 이 더 이상의 request 를 받지 않도록 한다.
		this.connector.pause();

		//Tomcat 이 request handling 을 위하여 사용중인 Executor 를 가져와 안전하게 셧다운한다
		Executor executor = this.connector.getProtocolHandler().getExecutor();
		if (executor instanceof ThreadPoolExecutor) {
			try {
				ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
				//request thread pool에 새로운 job 추가를 블락한다
				threadPoolExecutor.shutdown();

				//최대 TIMEOUT 동안 대기하며 threadpool의 모든 작업이 종료되었는지 확인한다. 만약 종료되지 못했다면 강제로 스탑한다.
				if (!threadPoolExecutor.awaitTermination(tomcatTerminationTimeoutMillis, TimeUnit.MILLISECONDS)) {
					log.warn(String.format("Tomcat thread pool did not shut down gracefully within %d seconds. Proceeding with forceful shutdown", tomcatTerminationTimeoutMillis));
					threadPoolExecutor.shutdownNow();
					if (!threadPoolExecutor.awaitTermination(tomcatTerminationTimeoutMillis, TimeUnit.MILLISECONDS)) {
						log.error("Tomcat thread pool did not terminated");
					}
				}
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			} finally {
				log.info("Tomcat web server shutdown COMPLETE");
			}
		} else {
			log.warn(String.format("Executor of Tomcat was not ThreadPoolExecutor. Executor was : %s. Skip Tomcat shutdown phase.", executor.getClass().toString()));
		}

		//Consumer thread pool을 정지한
		log.info("Shutting down all running consumer threads...");
		log.info("Current running consumer list is like as below");
		try {
			System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(consumerDataService.getConsumerMap()));
		} catch (JsonProcessingException e) {
			log.warn("JsonProcessingException thrown by System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(consumerDataService.getConsumerDataMap()))");
			log.warn("Proceeding shutdown job...");
		}

		for(Map.Entry<String, ConsumingPbftClient> entry : consumerDataService.getConsumerMap().entrySet()) {
			String topic = entry.getKey();
			ConsumingPbftClient consumer = entry.getValue();
			try {
				consumer.destroy();
				log.info(String.format("Wait for consumer graceful shutdown"));
				log.info(String.format("Consumer with topic : %s. destroy succeed", topic));
			} catch (Exception e) {
				log.error(String.format("Failed to destroy consumer instance. Topic : %s, cause : ", topic), e);
			}
		}
		log.info(String.format("Waiting consumer threads shutdown for %d ms...", consumerThreadAwaitTime));
		try {
			Thread.sleep(consumerThreadAwaitTime);
		} catch (InterruptedException ignore) {
		}
		log.info("Running consumer threads shutdown COMPLETE");
		/**
		 * 블럭화를 진행하는 Execute 메소드를 한개의 전용 쓰레드가 모두 처리하기 떄문에
		 * contextClosetEvent 가 발생하면, 즉 api 서버가 종료를 하면
		 * 해당 쓰레드가 작업중인지를 확인하고
		 * 작업이 끝나야만 쓰레드를 종료시킬 수 있도록 설정과
		 * 작업이 끝나기에 충분한 만큼의 타임아웃 시간을 설정 후
		 * shutdown 을 수행한다.
		 */
		log.info("Checking PBFT Execute Thread");
		ThreadPoolTaskExecutor threadPoolTaskExecutor = (ThreadPoolTaskExecutor)executeThreadPool;
		log.info(String.format("PBFT Execute Thread ActiveCount : %d",threadPoolTaskExecutor.getActiveCount()));
		threadPoolTaskExecutor.setAwaitTerminationSeconds(executeThreadAwaitTime);
		threadPoolTaskExecutor.shutdown();
		//최대 TIMEOUT 동안 대기하며 threadpool의 모든 작업이 종료되었는지 확인한다. 만약 종료되지 못했다면 강제로 스탑한다.
		log.info("PBFT Execute Service shutdown COMPLETE");
	}
}