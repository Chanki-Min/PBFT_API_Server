package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.broker.apiserver.Service.BlockVerificationSchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 테스트용 컨트롤러
 */
@Slf4j
@Controller(value = "/pbft")
public class PbftTestController {
	@Autowired
	BlockVerificationSchedulerService dynamicTaskScheduler;

	@RequestMapping(value = "/test")
	@ResponseBody
	public String test() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation op = new kr.ac.hongik.apl.Operations.Dev.GreetingOperation(client.getPublicKey());
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
		client.request(requestMessage);

		kr.ac.hongik.apl.Operations.Dev.Greeting reply = (kr.ac.hongik.apl.Operations.Dev.Greeting) client.getReply();
		client.close();

		return reply.greeting;
	}
}
