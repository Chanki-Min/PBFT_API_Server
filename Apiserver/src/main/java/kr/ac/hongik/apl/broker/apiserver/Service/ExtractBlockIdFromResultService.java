package kr.ac.hongik.apl.broker.apiserver.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.broker.apiserver.Pojo.BlockId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ExtractBlockIdFromResultService {
	private static final String HITS = "hits";
	private static final String _SOURCE = "_source";
	private static final String CHAIN_NAME = "chain_name";
	private static final String BLOCK_NUMBER = "block_number";

	private final ObjectMapper objectMapper;

	@Autowired
	public ExtractBlockIdFromResultService(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	/**
	 * Java stream api를 이용하여 elasticsearch 검색 결과에서 블록ID를 추출한다
	 *
	 * @param searchList elasticsearch에서 데이터를 쿼리하여 나온 raw result (json format)
	 * @return list of blockId that need to be verified
	 * @throws JsonProcessingException
	 */
	public List<BlockId> extractBlockId(String searchList) throws JsonProcessingException {

		Map<String, Object> allResultMap = objectMapper.readValue(searchList, Map.class);
		if(!allResultMap.containsKey(HITS))
			return null;

		List<Map<String, Object>> hitsList = (List<Map<String, Object>>) (((Map<String, Object>) (allResultMap.get(HITS))).get(HITS));

		return hitsList.stream()
				.map(map-> (Map<String, Object>) map.get(_SOURCE))
				.map(map -> new BlockId((String) map.get(CHAIN_NAME), (int) map.get(BLOCK_NUMBER)))
				.distinct()
				.collect(Collectors.toList());
	}
}
