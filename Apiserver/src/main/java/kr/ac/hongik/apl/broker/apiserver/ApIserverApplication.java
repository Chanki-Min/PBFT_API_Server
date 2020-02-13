package kr.ac.hongik.apl.broker.apiserver;

import kr.ac.hongik.apl.broker.apiserver.Pojo.BufferedConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerBufferConfigs;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerImmediateConfigs;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ImmediateConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerFactoryService;
import kr.ac.hongik.apl.broker.apiserver.Service.Sqlite.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@MapperScan(basePackages = "kr.ac.hongik.apl.broker.apiserver")
@SpringBootApplication
public class ApIserverApplication implements ApplicationRunner {
	private final StatusService statusService;
	private final ConsumerFactoryService consumerFactoryService;
	private final AsyncExecutionService asyncExecutionService;
	@Autowired
	public ApIserverApplication(StatusService statusService, ConsumerFactoryService consumerFactoryService,AsyncExecutionService asyncExecutionService) {
		this.statusService = statusService;
		this.consumerFactoryService = consumerFactoryService;
		this.asyncExecutionService = asyncExecutionService;
	}

	//TODO : shutdown 방법을 생각해야 한다 ref) https://www.baeldung.com/spring-boot-shutdown
	public static void main(String[] args) {
		SpringApplication.run(ApIserverApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		List<ConsumerBufferConfigs> bufferStatusList = statusService.getBufferStatus();

		for( ConsumerBufferConfigs config : bufferStatusList){
			log.info(config.getBuffTopicName().toString()+" is turned ON");
			BufferedConsumingPbftClient bufferedConsumingPbftClient =
					consumerFactoryService.MakeBufferedConsumer(config.getCommonConfigs(),config.getBuffConfigs());
			asyncExecutionService.runAsConsumerExecutor(bufferedConsumingPbftClient::startConsumer, null);
		}

		List<ConsumerImmediateConfigs> immediateConfigs = statusService.getImmediateStatus();

		for( ConsumerImmediateConfigs config : immediateConfigs){
			log.info(config.getImmediateTopicName().toString()+" is turned ON");
			ImmediateConsumingPbftClient immediateConsumingPbftClient =
					consumerFactoryService.MakeImmediateConsumer(config.getCommonConfigs(),config.getConImmeConfigs());
			asyncExecutionService.runAsConsumerExecutor(immediateConsumingPbftClient::startConsumer, null);
		}
	}
}
