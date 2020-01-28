package kr.ac.hongik.apl.broker.apiserver.Controller;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ElasticsearchIndexInfo;
import kr.ac.hongik.apl.broker.apiserver.Service.ElasticsearchIndexCreationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@Slf4j
@RestController(value = "/elasticsearch")
public class EsAdminController {
	@Autowired
	ElasticsearchIndexCreationService elasticsearchIndexCreationService;

	@RequestMapping(value = "/create/index", method = RequestMethod.POST)
	@ResponseBody
	public String createIndexRequest(@RequestBody ElasticsearchIndexInfo elasticsearchIndexInfo) throws EsRestClient.EsSSLException, NoSuchFieldException, IOException {
		return elasticsearchIndexCreationService.makeIndexToElasticsearch(elasticsearchIndexInfo);
	}
}
