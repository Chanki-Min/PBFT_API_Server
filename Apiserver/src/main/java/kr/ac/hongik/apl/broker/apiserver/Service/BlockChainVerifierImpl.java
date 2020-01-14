package kr.ac.hongik.apl.broker.apiserver.Service;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.GetLatestBlockNumberOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.OperationExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class BlockChainVerifierImpl implements BlockChainVerifier {

	@Autowired
	@Qualifier(value = "pbftClientProperties")
	private Properties properties;

	@Resource(name = "esRestClientConfigs")
	private Map<String, Object> esRestClientConfigs;

	public void verifyBlock() {
		//TODO : chainName을 Pbft한테 가져오기
		String chainName = "car_logs_blockChain";

		Client client = new Client(properties);

		Operation getLatestBlockNumberOp = new GetLatestBlockNumberOperation(client.getPublicKey(), chainName);
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), getLatestBlockNumberOp);

		client.request(requestMessage);
		Object latestBlockNumber = client.getReply();
		if(latestBlockNumber instanceof OperationExecutionException) {
			log.error(String.format("OperationExecutionException occurred, e : %s", latestBlockNumber));
			return;
		}
		log.info(String.format("got latest blockNumber from %s, blockNumber : %d", chainName, (int) latestBlockNumber));

		for(int blockNumber=1; blockNumber<(int) latestBlockNumber; blockNumber++) {
			Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), esRestClientConfigs, chainName, blockNumber);
			requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);

			client.request(requestMessage);
			Object verifyResult = client.getReply();

			if(verifyResult instanceof OperationExecutionException) {
				//log.error(String.format("OperationExecutionException occurred, e : %s", latestBlockNumber));
			} else {
				List<String> result = (List<String>) verifyResult;
				//TODO : 리절트 반환
				log.info("BlockVerification end, send to server, result : ");
				result.forEach(log::info);
			}
		}
	}
}
