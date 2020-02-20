package kr.ac.hongik.apl.broker.apiserver.Service.Blockchain;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.*;
import kr.ac.hongik.apl.broker.apiserver.Pojo.BlockId;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.SendAckToDesignatedURLService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * BlockChainVerifier 인터페이스의 구현체로, 현재 PBFT에 로드된 블록체인 리스트를 받아와서 각각의 모든 블록을 loop로 검증하고,
 * 결과를 logging, 모니터링 서버에 전송합니다
 */
@Slf4j
@Component
public class BlockChainVerifierImpl implements BlockChainVerifier {

	private final Properties properties;
	private final SendAckToDesignatedURLService sendAckToDesignatedURLService;
	/*
	* 컬렉션 타입의 객체는 Autowired를 통한 DI가 정상적으로 동작하지 않는다 (Key로 value ="~"을 가지고 Value로 해당 맵을 가지는 맵이 생성됨)
	* 따라서 컬렉션 타입을 반환하는 Bean method를 DI할때는 Resource 어노테이션을 사용해야 한다
	*/
	@Resource(name = "esRestClientConfigs")
	private Map<String, Object> esRestClientConfigs;

	@Autowired
	public BlockChainVerifierImpl(@Qualifier(value = "pbftClientProperties") Properties properties, SendAckToDesignatedURLService sendAckToDesignatedURLService) {
		this.properties = properties;
		this.sendAckToDesignatedURLService = sendAckToDesignatedURLService;
	}

	/**
	 * 전체 블록체인을 검증하는 메소드
	 */
	public void verifyBlockChain() {
		try (Client client = new Client(properties)) {
			Operation getBlockChainListOp = new GetBlockChainListOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), getBlockChainListOp);
			client.request(requestMessage);
			Object blockChainList = client.getReply();
			if (blockChainList instanceof OperationExecutionException) {
				log.error("OperationExecutionException occurred", (OperationExecutionException) blockChainList);
				return;
			}
			List<String> chainList = (List<String>) blockChainList;
			System.out.println(String.format("got chainlist, %d", chainList.size()));

			for (String chainName: chainList) {

				Operation getLatestBlockNumberOp = new GetLatestBlockNumberOperation(client.getPublicKey(), chainName);
				requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), getLatestBlockNumberOp);
				client.request(requestMessage);
				Object latestBlockNumber = client.getReply();
				if (latestBlockNumber instanceof OperationExecutionException) {
					log.error("OperationExecutionException occurred, e : %s", (OperationExecutionException) latestBlockNumber);
					return;
				} else {
					log.info(String.format("got latest blockNumber from %s, blockNumber : %d", chainName, (int) latestBlockNumber));
				}
				System.out.println("blocknum");

				for (int blockNumber = 1; blockNumber < (int) latestBlockNumber; blockNumber++) {
					Operation verifyBlockOp = new VerifyBlockOperation(client.getPublicKey(), esRestClientConfigs, chainName, blockNumber);
					requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);

					client.request(requestMessage);
					Object verifyResult = client.getReply();

					if (verifyResult instanceof OperationExecutionException) {
						log.error("OperationExecutionException occurred ", (OperationExecutionException) verifyResult);
					} else {
						List<String> result = (List<String>) verifyResult;
						sendAckToDesignatedURLService.sendVerificationLog(result);
						log.debug("BlockVerification end, send to server, result : ");
						result.forEach(log::info);
					}
				}
			}
		}
	}

	/**
	 * 인자로 받은 chainName-blockNumber 리스트에 있는 블록들만 검증하는 메소드 (검색 기능에서 사용)
	 *
	 * @param blockIdList 검증할 블록의 정보 리스트
	 */
	@Override
	public void verifyDesignatedBlockList(List<BlockId> blockIdList) {
		try (Client client = new Client(properties)) {
			for(BlockId blockId : blockIdList) {
				Operation verifyBlockOp = new VerifyBlockOperation(client.getPublicKey(), esRestClientConfigs, blockId.getChainName(), blockId.getBlockNumber());
				RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
				client.request(requestMessage);

				Object verifyResult = client.getReply();
				if (verifyResult instanceof OperationExecutionException) {
					log.error("OperationExecutionException occurred ", (OperationExecutionException) verifyResult);
				} else {
					List<String> result = (List<String>) verifyResult;
					sendAckToDesignatedURLService.sendVerificationLog(result);
					log.debug("BlockVerification end, send to server, result : ");
					result.forEach(log::info);
				}
			}
		}
	}
}
