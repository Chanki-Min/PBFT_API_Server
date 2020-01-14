package kr.ac.hongik.apl.broker.apiserver.Configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
public class PbftClientConfiguration {

	@Bean(name = "pbftClientProperties")
	public Properties pbftClientProperties() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties properties = new Properties();
		properties.load(in);
		return properties;
	}

	@Bean(name = "esRestClientConfigs")
	public HashMap<String, Object> esRestClientConfigs() {
		HashMap<String, Object> esRestClientConfigs = new HashMap<>();
		esRestClientConfigs.put("userName", "apl");
		esRestClientConfigs.put("passWord", "wowsan2015@!@#$");
		esRestClientConfigs.put("certPath", "/ES_Connection/esRestClient-cert.p12");
		esRestClientConfigs.put("certPassWord", "wowsan2015@!@#$");
		Map<String, Object> masterMap = new HashMap<>();
		masterMap.put( "name", "es01-master01");
		masterMap.put( "hostName", "223.194.70.105");
		masterMap.put( "port", "19192");
		masterMap.put( "hostScheme", "https");
		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
		return esRestClientConfigs;
	}

}
