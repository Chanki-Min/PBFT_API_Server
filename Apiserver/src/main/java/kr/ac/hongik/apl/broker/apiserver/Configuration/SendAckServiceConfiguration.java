package kr.ac.hongik.apl.broker.apiserver.Configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SendAckServiceConfiguration {
	public static final String ACK_BLOCK_GENERATION = "broker.ack.block.generation.url";
	public static final String ACK_VERIFICATION = "broker.ack.verification.url";
	private final Environment env;

	@Autowired
	public SendAckServiceConfiguration(Environment env) {
		this.env = env;
	}

	/**
	 * @return broker.properties 파일에서 정의된 각 ack url이 value로 담긴 Map
	 */
	private Map<String, String> sendAckServiceUrlMap() {
		Map<String, String> configMap = new HashMap<>();
		configMap.put(ACK_BLOCK_GENERATION, env.getProperty(ACK_BLOCK_GENERATION, String.class));
		configMap.put(ACK_VERIFICATION, env.getProperty(ACK_VERIFICATION, String.class));
		return configMap;
	}

	/**
	 * @return key : static string key, value : 초기화되어 사용 준비된 WebClient 의 Map
	 */
	@Bean(name = "responseWebClientMap")
	public Map<String, WebClient> responseWebClientMap() {
		Map<String, String> sendAckServiceUrlMap = sendAckServiceUrlMap();

		Map<String, WebClient> responseWebClientMap = new HashMap<>();
		for(Map.Entry<String, String> entry: sendAckServiceUrlMap.entrySet()) {
			responseWebClientMap.put(entry.getKey(), WebClient.builder()
					.baseUrl(entry.getValue())
					.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
					.build()
			);
		}
		return responseWebClientMap;
	}
}
