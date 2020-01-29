package kr.ac.hongik.apl.broker.apiserver.Service.Elasticsearch;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ElasticsearchQuery;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

@Service
public class ElasticsearchSearchService {
	@Resource(name = "esRestClientConfigs")
	Map<String, Object> esRestClientConfigs;

	/**
	 * 인자로 검색에 필요한 인자를 받아 검색을 수행한다, 수행한 결과를 반환한다
	 *
	 * @exception Exception 예외는 service를 사용하는 컨트롤러가 핸들한다
	 * @param elasticsearchQuery elasticsearch에게서 low-level rest 검색을 하기 위하여 필요한 값들의 Pojo
	 * @return elasticsearch에게서 검색하여 받아온 http Entity의 toString()값
	 */
	public String searchFromElasticsearch(ElasticsearchQuery elasticsearchQuery) throws EsRestClient.EsSSLException, NoSuchFieldException, IOException {
		try (EsRestClient esRestClient = new EsRestClient(esRestClientConfigs)) {
			esRestClient.connectToEs();
			Request request = new Request(elasticsearchQuery.getHttpProtocol(), elasticsearchQuery.getEndPoint());
			elasticsearchQuery.getParameters().forEach(request::addParameter);
			request.setEntity(
					new NStringEntity(
							elasticsearchQuery.getRequestBody(),
							ContentType.APPLICATION_JSON
					)
			);
			Response response = esRestClient.getClient().getLowLevelClient().performRequest(request);
			return EntityUtils.toString(response.getEntity());
		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException e) {
			throw e;
		}
	}
}
