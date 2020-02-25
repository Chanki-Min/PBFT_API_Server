package kr.ac.hongik.apl.broker.apiserver.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.BlockId;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ChangeCronScheduleResponse;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ElasticsearchQuery;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Blockchain.BlockChainVerifier;
import kr.ac.hongik.apl.broker.apiserver.Service.Blockchain.BlockChainVerifierImpl;
import kr.ac.hongik.apl.broker.apiserver.Service.Blockchain.BlockVerificationSchedulerService;
import kr.ac.hongik.apl.broker.apiserver.Service.Elasticsearch.ElasticsearchSearchService;
import kr.ac.hongik.apl.broker.apiserver.Service.Elasticsearch.ExtractBlockIdFromResultService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@Slf4j
@Controller(value = "/blockchain")
public class BlockchainController {
	private static final String SEARCH_ERROR_MSG = "executing query to elasticsearch failed. reason : ";
	private static final String RESULT_EXTRACTION_ERROR_MSG = "extracting blockIds from searchResult failed, abort verification. searchResult : \n%s\n";

	private final BlockVerificationSchedulerService blockVerificationSchedulerService;
	private final ElasticsearchSearchService elasticsearchSearchService;
	private final ExtractBlockIdFromResultService extractBlockNumberFromResultService;
	private final AsyncExecutionService asyncExecutionService;
	private final BlockChainVerifierImpl blockChainVerifier;

	@Autowired
	public BlockchainController(BlockVerificationSchedulerService blockVerificationSchedulerService, ElasticsearchSearchService elasticsearchSearchService, ExtractBlockIdFromResultService extractBlockNumberFromResultService, AsyncExecutionService asyncExecutionService, BlockChainVerifierImpl blockChainVerifier) {
		this.blockVerificationSchedulerService = blockVerificationSchedulerService;
		this.elasticsearchSearchService = elasticsearchSearchService;
		this.extractBlockNumberFromResultService = extractBlockNumberFromResultService;
		this.asyncExecutionService = asyncExecutionService;
		this.blockChainVerifier = blockChainVerifier;
	}

	/**
	 * 현재 BlockVerificationSchedulerService에 스캐쥴링된 task를 취소하고, 새로운 cron으로 task를 스캐쥴링합니다
	 *
	 * @param cron 6자리로 구성된 Cron
	 * @return 설정된 cron
	 */
	@RequestMapping(value = "/changeVerificationSchedule")
	@ResponseBody
	public ChangeCronScheduleResponse changeCronSchedule(@RequestParam(name = "cron", required = true) String cron) {
		blockVerificationSchedulerService.changeCronSchedule(cron);

		return new ChangeCronScheduleResponse(String.format("Verification schedule changed to %s", cron), cron);
	}

	/**
	 * 인자로 받은 쿼리의 결과를 동기적으로 반환하고, 쿼리의 결과에서 추출된 블록체인의 각 블록들을 스레드 풀에서 검증하여 결과를
	 * pre-define 된 서버로 전송한다
	 *
	 * @param elasticsearchQuery 미리 정의된 형식의 elasticsearch low-level queury 객체
	 * @return elasticsearch 검색 결과 (json)
	 * @throws EsRestClient.EsSSLException
	 * @throws NoSuchFieldException
	 * @throws IOException
	 */
	@RequestMapping(value = "/search", method = RequestMethod.POST)
	@ResponseBody
	public String searchAndVerifyData(@RequestBody ElasticsearchQuery elasticsearchQuery) {
		String searchResult;
		try {
			searchResult = elasticsearchSearchService.searchFromElasticsearch(elasticsearchQuery);
		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException e) {
			log.error(SEARCH_ERROR_MSG, e);
			return String.format(SEARCH_ERROR_MSG + "%s", e);
		}

		try {
			List<BlockId> includedBlockNumberList = extractBlockNumberFromResultService.extractBlockId(searchResult);
			/*
			 * 검색 결과에서 추출된 List{tuple(chainName : blockNumber)}에 대하여 (단독 블럭 검증을 수행하고, 결과를 SP 서버에 전송하는 메소드) 를 비동기로 호출한다
			 */
			asyncExecutionService.runAsSearchResultVerifierExecutor(
					() -> {
						blockChainVerifier.verifyDesignatedBlockList(includedBlockNumberList);
					}
			);
			return searchResult;
		} catch (JsonProcessingException e) {
			log.error(String.format(RESULT_EXTRACTION_ERROR_MSG, searchResult), e);
			return String.format(RESULT_EXTRACTION_ERROR_MSG, searchResult);
		}
	}
}
