package kr.ac.hongik.apl.broker.apiserver.Service;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Map;

public interface ConsumingPbftClient extends InitializingBean, DisposableBean {

	public void startConsumer();
	public void shutdownConsumer();
	@Override
	void afterPropertiesSet() throws Exception;
	@Override
	void destroy() throws Exception;
}
