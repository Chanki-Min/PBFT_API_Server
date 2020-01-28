package kr.ac.hongik.apl.broker.apiserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApIserverApplication {
	//TODO : shutdown 방법을 생각해야 한다 ref) https://www.baeldung.com/spring-boot-shutdown
	public static void main(String[] args) {
		SpringApplication.run(ApIserverApplication.class, args);
	}

}
