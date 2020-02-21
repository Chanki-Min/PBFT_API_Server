package kr.ac.hongik.apl.broker.apiserver.Configuration;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.broker.apiserver.ApIserverApplication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.boot.env.PropertySourceLoader;
import org.springframework.boot.system.ApplicationHome;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.util.*;

@Slf4j
@Configuration
public class PbftClientConfiguration {
	private static final String REPLICA_PROPERTIES_DIR = "replica.properties";

	public static final String ELASTICSEARCH_CERT_PATH = "broker.elasticsearch.cert.path";
	public static final String ELASTICSEARCH_CONNECT_NODES_COUNT = "broker.elasticsearch.connect_nodes_count";
	public static final String ELASTICSEARCH_NODE_NAMES = "broker.elasticsearch.node.names";
	public static final String ELASTICSEARCH_NODE_HOSTS = "broker.elasticsearch.node.hosts";
	public static final String ELASTICSEARCH_NODE_PORTS = "broker.elasticsearch.node.ports";
	public static final String ELASTICSEARCH_NODE_SCHEME = "broker.elasticsearch.node.scheme";

	private final PropertySourceLoader loader = new PropertiesPropertySourceLoader();

	private final Environment env;

	@Autowired
	public PbftClientConfiguration(Environment env) {
		this.env = env;
	}

	@Bean(name = "pbftClientProperties")
	public Properties pbftClientProperties() throws IOException {
		Properties properties = new Properties();
		InputStream inputStream = new ClassPathResource("resourcesPath.properties", this.getClass().getClassLoader()).getInputStream();
		properties.load(inputStream);
		String currentJarPath = getCurrentSpringJarFileDir();
		String relativePath = properties.getProperty(REPLICA_PROPERTIES_DIR);

		properties = new Properties();
		try {
			Resource resource = new FileSystemResource(currentJarPath+relativePath);
			properties.load(resource.getInputStream());
		} catch (FileNotFoundException e) {
			String intellijProjectDir = getCurrentIntellijProjectDir();
			Resource resource = new FileSystemResource(intellijProjectDir+relativePath);
			properties.load(resource.getInputStream());
		}
		return properties;
	}

	@Bean(name = "esRestClientConfigs")
	public HashMap<String, Object> esRestClientConfigs() throws IOException {
		//TODO : PBFT에게 인증서의 경로를 보내는 것은 의미가 없다. 다른 방법을 찾아볼 필요가 있다.

		HashMap<String, Object> esRestClientConfigs = new HashMap<>();

		esRestClientConfigs.put("userName", getStdinFromConsole("Enter elasticsearch username : ", false));
		esRestClientConfigs.put("passWord", getStdinFromConsole("Enter elasticsearch password : ", true));

		esRestClientConfigs.put("certPassWord", getStdinFromConsole("Enter elasticsearch certificate password : ", true));
		esRestClientConfigs.put("certPath", env.getProperty(ELASTICSEARCH_CERT_PATH));

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

		try(EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);) {

			esRestClient.connectToEs();
			esRestClient.getClusterInfo();
		} catch (Exception e) {
			log.error(e.getMessage());
			System.exit(0);
		}
		return esRestClientConfigs;
	}

	/**
	 * ID,PW 등의 정보를 STDIN에서 읽어와 반환한다. java.io 의 Console 클래스를 통하여 각 시스템의 콘솔에서 echo 없이 안전하게 비밀번호를 가져올
	 * 수 있지만, Console 객체는 쉘이 아닌 곳 (IDE 등)에서 실행시 null이 반환되며 사용할 수 없는 문제점이 있다.
	 * 이 문제점을 해결하기 위하여 이 메소드는 Console 이 null일시 마스킹을 포기하고 scanner 를 사용한 입력을 받는다.
	 *
	 * @param fmt 프롬프트 스트링
	 * @param hideInput 가능할 경우 입력받는 문자열의 echo를 차단한다
	 * @return STDIN에서 읽어온 문자열
	 */
	private String getStdinFromConsole(String fmt, boolean hideInput) {
		Console console = System.console();
		if(console != null) {
			if(hideInput) {
				return String.valueOf(console.readPassword(fmt));
			} else {
				return console.readLine(fmt);
			}

		} else {
			Scanner scanner = new Scanner(System.in);
			System.out.print(fmt);
			return scanner.nextLine();
		}
	}

	private String getCurrentSpringJarFileDir() {
		ApplicationHome home = new ApplicationHome(ApIserverApplication.class);
		try {
			return home.getDir().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String getCurrentIntellijProjectDir() {
		File jarDir = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		try {
			return jarDir.getParentFile().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
