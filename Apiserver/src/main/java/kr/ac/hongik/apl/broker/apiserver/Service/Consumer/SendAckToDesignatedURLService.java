package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SendAckToDesignatedURLService {

    private final WebClient.Builder webClientBuild;
    private final WebClient webClient;
    private String url = "http://127.0.0.1:8081"; // 통신할 서버의 url 주소
    private String blockUri = "/restLast"; //sendLastData의 mapping 주소
    private String validateUri = "/restResult"; //sendVerificationLog의 mapping 주소


    @Autowired
    public SendAckToDesignatedURLService(WebClient.Builder webClientBuild) {
        this.webClientBuild = webClientBuild;
        webClient = webClientBuild.baseUrl(url)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * locke server에 Ack를 보내고 응답 string(s)을 출력하는 메소드
     *
     * @param last : consume한 last data
     */
    //TODO : 요청을 보낼 URI를 가져와야 합니다
    public void sendLastData(Map<String, Object> last)  {

        Map<String, Object> map = new HashMap<>();
        map.put("data",last);

        WebClient.RequestHeadersSpec requestSpec = webClient
                .post()
                .uri(blockUri)
                .bodyValue(map);

        Mono<String> respoMono = requestSpec
                .retrieve()
                .bodyToMono(String.class);
        respoMono.doOnSuccess(s -> {
            System.out.println(s); //response
        }).subscribe();
    }

    /**
     * 검증 후 locke server에 Ack를 보내고 응답 string(s)을 출력하는 메소드
     *
     * @param result : 검증 결과 (json형식의 string으로 된 List)
     */
    public void sendVerificationLog(List<String> result) {

        Map<String, Object> map = new HashMap<>();
        map.put("data",result);

        WebClient.RequestHeadersSpec requestSpec = webClient
                .post()
                .uri(validateUri)
                .bodyValue(map);

        Mono<String> respoMono = requestSpec
                .retrieve()
                .bodyToMono(String.class);
        respoMono.doOnSuccess(s -> {
            System.out.println(s); //response
        }).subscribe();
    }
}




