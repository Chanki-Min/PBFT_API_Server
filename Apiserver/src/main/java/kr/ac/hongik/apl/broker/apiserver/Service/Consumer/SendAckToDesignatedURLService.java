package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kr.ac.hongik.apl.broker.apiserver.Configuration.SendAckServiceConfiguration.ACK_BLOCK_GENERATION;
import static kr.ac.hongik.apl.broker.apiserver.Configuration.SendAckServiceConfiguration.ACK_VERIFICATION;

@Slf4j
@Service
public class SendAckToDesignatedURLService {

    @Resource(name = "responseWebClientMap")
    private Map<String, WebClient> responseWebClientMap;

    /**
     * locke server에 Ack를 보내고 응답 string(s)을 출력하는 메소드
     *
     * @param last : consume한 last data
     */
    public void sendLastData(Map<String, Object> last)  {
        Map<String, Object> map = new HashMap<>();
        map.put("data",last);

        WebClient.RequestHeadersSpec<?> requestSpec = responseWebClientMap.get(ACK_BLOCK_GENERATION)
                .post()
                .bodyValue(map);

        Mono<String> respoMono = requestSpec
                .retrieve()
                .bodyToMono(String.class);
        //response
        respoMono.doOnSuccess(System.out::println).subscribe();
    }

    /**
     * 검증 후 locke server에 Ack를 보내고 응답 string(s)을 출력하는 메소드
     *
     * @param result : 검증 결과 (json형식의 string으로 된 List)
     */
    public void sendVerificationLog(List<String> result) {
        Map<String, Object> map = new HashMap<>();
        map.put("data",result);

        WebClient.RequestHeadersSpec<?> requestSpec = responseWebClientMap.get(ACK_VERIFICATION)
                .post()
                .bodyValue(map);

        Mono<String> respoMono = requestSpec
                .retrieve()
                .bodyToMono(String.class);
        //response
        respoMono.doOnSuccess(System.out::println).subscribe();
    }
}




