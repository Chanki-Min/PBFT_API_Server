package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
public class ElasticsearchIndexInfo {
	/**
	 * 생성할 인덱스 이름
	 */
	String indexName;
	/**
	 * 인덱스 매핑 정보
	 * ref) https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
	 */
	Map<String, Object> mapping;
	/**
	 * 인덱스 세팅 정보
	 * ref) https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-modules-settings
	 */
	Map<String, Object> setting;
}
