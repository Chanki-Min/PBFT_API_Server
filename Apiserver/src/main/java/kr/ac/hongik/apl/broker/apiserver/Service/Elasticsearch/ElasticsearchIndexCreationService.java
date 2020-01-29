package kr.ac.hongik.apl.broker.apiserver.Service.Elasticsearch;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ElasticsearchIndexInfo;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

@Slf4j
@Service
public class ElasticsearchIndexCreationService {

	@Resource(name = "esRestClientConfigs")
	Map<String, Object> esRestClientConfigs;

	public String makeIndexToElasticsearch(ElasticsearchIndexInfo indexInfo) throws IOException, NoSuchFieldException, EsRestClient.EsSSLException, ElasticsearchException {
			try (EsRestClient esRestClient = new EsRestClient(esRestClientConfigs)) {
				esRestClient.connectToEs();

				CreateIndexRequest request = new CreateIndexRequest(indexInfo.getIndexName());
				request.mapping(indexInfo.getMapping());
				request.settings(indexInfo.getSetting());

				CreateIndexResponse response = esRestClient.getClient().indices().create(request, RequestOptions.DEFAULT);
				return Strings.toString(response);
			}
	}
}
