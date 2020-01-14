package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.ES.EsRestClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController(value = "/elasticsearch")
public class EsAdminController {
	/*
	* EsRestClient를 AutoWire한다
	* @Autowired
	* EsRestClient esRestClient;
	*/

	@RequestMapping(value = "/create/index", method = RequestMethod.POST)
	public String createIndexRequest() {
		/*
		인덱스 매핑, 세팅, es 계정정보를 가져와서 esRestClient로 인덱스를 생성하고, 생성된 매핑을 반환합니다
		 */
		throw new NotImplementedException("구현합시다");
	}
}
