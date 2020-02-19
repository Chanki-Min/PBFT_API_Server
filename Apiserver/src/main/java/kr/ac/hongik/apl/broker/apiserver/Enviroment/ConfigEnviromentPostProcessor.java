package kr.ac.hongik.apl.broker.apiserver.Enviroment;

import kr.ac.hongik.apl.broker.apiserver.ApIserverApplication;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.boot.env.PropertySourceLoader;
import org.springframework.boot.system.ApplicationHome;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

public class ConfigEnviromentPostProcessor implements EnvironmentPostProcessor {

	private final PropertySourceLoader loader = new PropertiesPropertySourceLoader();

	@SneakyThrows
	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		System.out.println("Environment instance ready. loading config files...");
		Properties properties = new Properties();
		InputStream inputStream = new ClassPathResource("resourcesPath.properties", this.getClass().getClassLoader()).getInputStream();
		properties.load(inputStream);
		String currentJarDir = getCurrentSpringJarFileDir();
		System.out.println(String.format("Loading path = %s%s", currentJarDir, "/config"));


		for(Entry<Object, Object> prop : properties.entrySet()) {
			String relativePath = (String) prop.getValue();
			String absolutePath = currentJarDir + relativePath;
			System.out.println(String.format("Loading %s", relativePath));

			Resource resource = new FileSystemResource(absolutePath);
			PropertySource<?> propertySource;
			try {
				propertySource = loader.load((String) prop.getKey(), resource).get(0);
			} catch (IOException e) {
				String absoluteIntellijPath = getCurrentIntellijProjectDir() + relativePath;
				System.out.println("\nFailed to load resource file. trying to load as Intellij IDEA project...");
				System.out.println(String.format("Loading %s from Project path", absoluteIntellijPath));
				resource = new FileSystemResource(absoluteIntellijPath);
				propertySource = loader.load((String) prop.getKey(), resource).get(0);
			}
			environment.getPropertySources().addLast(propertySource);
		}
		System.out.println("Config file loading COMPLETE");
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
