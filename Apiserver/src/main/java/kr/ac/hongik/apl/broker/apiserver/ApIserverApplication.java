package kr.ac.hongik.apl.broker.apiserver;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * ApplicationRunner Interface 를 이용하여 빈 주입 이후 최초 실행되어야하는 로직을 구현함.
 * DB 에 저장되어있는 (실행되어야 할) Consumer 들의 Configs 들을 불러와 factoryService 를 이용하여 Consumer 객체를 생성한다.
 * ConsumerDataService 객체에 해당 데이터를 저장하여 '실행후 생성과정없이' CONFIG 변경이나 CONSUMER 삭제를 시도하는 경우 발생할 예외를 방지하였다.
 *
 * @author 최상현
 */
@Slf4j
@MapperScan(basePackages = "kr.ac.hongik.apl.broker.apiserver")
@SpringBootApplication
public class ApIserverApplication implements ApplicationRunner {

	ApplicationContext applicationContext;

	@Autowired
	public ApIserverApplication(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	//TODO : shutdown 방법을 생각해야 한다 ref) https://www.baeldung.com/spring-boot-shutdown
	public static void main(String[] args) {
		SpringApplication.run(ApIserverApplication.class, args);
	}

	/**
	 * Event start 하면 ContextStartedEvent : ApplicationContext를 start()하여 라이프사이클 빈들이 시작 신호를 받은 시점에 발생
	 * @see kr.ac.hongik.apl.broker.apiserver.Service.EventListener.ContextStartedEventListener
	 */
	@Override
	public void run(ApplicationArguments args) {
		((ConfigurableApplicationContext)applicationContext).start();
	}
}
