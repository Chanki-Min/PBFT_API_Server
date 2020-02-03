package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;


import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SendBlockInsertionAckService {

    /**
     * sp server에 Ack를 보내고 상태 정보 받아옴
     *
     * @param last : consume한 last data
     */
    //TODO : 요청을 보낼 URI를 가져와야 합니
    public void sendLastData(Map<String, Object> last)  {

        // request url
        String url = null;

        if(url == null) {
            log.warn("reply url is null");
            return;
        }

        RestTemplate restTemplate = new RestTemplate();

        // header만들고 setting
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));


        // RestTemplate에 MessageConverter 세팅
        List<HttpMessageConverter<?>> converters = new ArrayList<HttpMessageConverter<?>>();
        converters.add(new FormHttpMessageConverter());
        converters.add(new StringHttpMessageConverter());
        converters.add(new MappingJackson2HttpMessageConverter());

        restTemplate.setMessageConverters(converters);


        // parameter 세팅
        MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("data",last);

        //보내는 데이터(last data) 확인
        log.info(map.getFirst("data").toString());

        // send POST request
        HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(map, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

        // check response
        if (response.getStatusCode() == HttpStatus.CREATED) {
            System.out.println("Request Successful");
            System.out.println(response.getBody());
        } else {
            System.out.println("Request Failed");
            System.out.println(response.getStatusCode());
        }

    }
}
