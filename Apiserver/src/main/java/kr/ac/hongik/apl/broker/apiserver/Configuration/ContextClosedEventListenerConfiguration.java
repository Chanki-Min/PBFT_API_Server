package kr.ac.hongik.apl.broker.apiserver.Configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ContextClosedEventListenerConfiguration {
	public static final String TOMCAT_TERMINATION_TIMEOUT_MILLIS = "broker.contextClosedEventListenerConfig.tomcat.timeout.millis";
	public static final String EXECUTE_THREAD_AWIATTIME = "broker.execute.thread.awaitTime";
	private final Environment env;

	@Autowired
	public ContextClosedEventListenerConfiguration(Environment env) {
		this.env = env;
	}

	@Bean(name = "contextClosedEventListenerConfig")
	public Map<String, Object> contextClosedEventListenerConfig() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put(TOMCAT_TERMINATION_TIMEOUT_MILLIS, env.getProperty(TOMCAT_TERMINATION_TIMEOUT_MILLIS, int.class));
		configMap.put(EXECUTE_THREAD_AWIATTIME, env.getProperty(EXECUTE_THREAD_AWIATTIME, int.class));

		return configMap;
	}
}
