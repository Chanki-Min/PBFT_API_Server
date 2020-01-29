package kr.ac.hongik.apl.broker.apiserver.Service.Blockchain;

import kr.ac.hongik.apl.broker.apiserver.Pojo.BlockId;

import java.util.List;

public interface BlockChainVerifier {

	/**
	 * 전체 블록체인을 검증하는 메소드
	 */
	void verifyBlockChain();

	/**
	 * 인자로 받은 chainName-blockNumber 리스트에 있는 블록들만 검증하는 메소드 (검색 기능에서 사용)
	 *
	 * @param blockIdList 검증할 블록의 정보 리스트
	 */
	void verifyDesignatedBlockList(List<BlockId> blockIdList);
}
