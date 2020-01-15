package kr.ac.hongik.apl.broker.apiserver.Configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Slf4j
@Configuration
@PropertySource("classpath:broker.properties")
public class PbftClientConfiguration {
	public static final String ELASTICSEARCH_CERT_PATH = "broker.elasticsearch.cert.path";
	public static final String ELASTICSEARCH_CONNECT_NODES_COUNT = "broker.elasticsearch.connect_nodes_count";
	public static final String ELASTICSEARCH_NODE_NAMES = "broker.elasticsearch.node.names";
	public static final String ELASTICSEARCH_NODE_HOSTS = "broker.elasticsearch.node.hosts";
	public static final String ELASTICSEARCH_NODE_PORTS = "broker.elasticsearch.node.ports";
	public static final String ELASTICSEARCH_NODE_SCHEME = "broker.elasticsearch.node.scheme";

	@Autowired
	Environment env;

	@Bean(name = "pbftClientProperties")
	public Properties pbftClientProperties() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties properties = new Properties();
		properties.load(in);
		return properties;
	}

	@Bean(name = "esRestClientConfigs")
	public HashMap<String, Object> esRestClientConfigs() {
		Scanner scanner = new Scanner(System.in);
		HashMap<String, Object> esRestClientConfigs = new HashMap<>();
		log.info("Enter elasticsearch username");
		esRestClientConfigs.put("userName", scanner.next());
		log.info("Enter elasticsearch password");
		esRestClientConfigs.put("passWord", scanner.next());
		esRestClientConfigs.put("certPath", env.getProperty(ELASTICSEARCH_CERT_PATH));
		log.info("Enter elasticsearch certificate password");
		esRestClientConfigs.put("certPassWord", scanner.next());

		int elasticsearchNodeCount = Integer.parseInt(Objects.requireNonNull(env.getProperty(ELASTICSEARCH_CONNECT_NODES_COUNT)));
		String[] elasticsearchNames = Objects.requireNonNull(env.getProperty(ELASTICSEARCH_NODE_NAMES, String[].class));
		String[] elasticsearchHosts = Objects.requireNonNull(env.getProperty(ELASTICSEARCH_NODE_HOSTS, String[].class));
		String[] elasticsearchPorts = Objects.requireNonNull(env.getProperty(ELASTICSEARCH_NODE_PORTS, String[].class));
		String elasticsearchScheme = Objects.requireNonNull(env.getProperty(ELASTICSEARCH_NODE_SCHEME));

		List<Map<String, Object>> masterHostInfo = new ArrayList<>();
		for(int i=0; i<elasticsearchNodeCount; i++) {
			Map<String, Object> masterMap = new HashMap<>();
			masterMap.put( "name", elasticsearchNames[i]);
			masterMap.put( "hostName", elasticsearchHosts[i]);
			masterMap.put( "port", elasticsearchPorts[i]);
			masterMap.put( "hostScheme", elasticsearchScheme);
			masterHostInfo.add(masterMap);
		}
		esRestClientConfigs.put("masterHostInfo", masterHostInfo);
		return esRestClientConfigs;
	}
}
