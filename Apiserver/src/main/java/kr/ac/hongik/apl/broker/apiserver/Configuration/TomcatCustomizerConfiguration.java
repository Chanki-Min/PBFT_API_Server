package kr.ac.hongik.apl.broker.apiserver.Configuration;

import kr.ac.hongik.apl.broker.apiserver.Service.EventListener.ContextClosedEventListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class TomcatCustomizerConfiguration {
	private final ContextClosedEventListener contextClosedEventListener;

	@Autowired
	public TomcatCustomizerConfiguration(ContextClosedEventListener contextClosedEventListener) {
		this.contextClosedEventListener = contextClosedEventListener;
	}

	@Bean
	public ConfigurableServletWebServerFactory webServerFactory() {
		TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
		factory.addConnectorCustomizers(contextClosedEventListener);
		return factory;
	}
}
