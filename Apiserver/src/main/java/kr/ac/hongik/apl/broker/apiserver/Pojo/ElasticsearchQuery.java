package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Getter @Setter
public class ElasticsearchQuery {
	/**
	 * http method의 string
	 * ref) https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	 */
	String httpProtocol;
	/**
	 * 검색시 넣을 엔드포인트
	 * ref) https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	 */
	String endPoint;
	/**
	 * 검색시 추가로 넣을 파라메터
	 * ref) https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	 */
	Map<String, String> parameters;
	/**
	 * json formatted string 쿼리
	 * ref) https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
	 */
	String requestBody;
}
