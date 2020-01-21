package kr.ac.hongik.apl.broker.apiserver.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class BufferedConsumingPbftClient implements ConsumingPbftClient {
    public static final String TOPICS = "kafka.listener.service.topic"; // lee2는 imm
    public static final String MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
    private final AtomicBoolean closed = new AtomicBoolean(false);

    //TODO : 이 값은 PBFT에 1차 해쉬도 삽입할지를 결정한다. 설정 값 또는 다른 방식으로 받을 필요가 있다.
    private boolean isHashListInclude = false;
    private int TIMEOUT_MILLIS = 5000;
    private int POLL_INTERVAL_MILLIS = 1000;
    private KafkaConsumer<String, Object> consumer = null;

    @Resource(name = "consumerConfigs")
    private Map<String, Object> consumerConfigs;
    @Resource(name = "bufferClientConfigs")
    private Map<String, Object> listerServiceConfigs;
    @Resource(name = "pbftClientProperties")
    private Properties pbftClientProperties;
    @Resource(name = "esRestClientConfigs")
    public HashMap<String, Object> esRestClientConfigs;

    //Async 노테이션의 메소드는 this가 invoke()할 수 없기 때문에 비동기 실행만 시키는 서비스를 주입한다
    @Autowired
    private AsyncExecutionService asyncExecutionService;
    @Autowired
    private ObjectMapper objectMapper;


    public BufferedConsumingPbftClient() {
    }
    @Override
    @Async("consumerThreadPool")
    public void startConsumer() {
        try {
            //consumerConfigs.replace(ConsumerConfig.GROUP_ID_CONFIG,"Lee");
            log.info("Start ConsumingPbftClientBuffer service");
            consumer = new KafkaConsumer<>(consumerConfigs);
            consumer.subscribe((Collection<String>) listerServiceConfigs.get(TOPICS));

            //TODO : 이 컨슈머는 각 토픽당 파티션이 1개라는 가정 하에 동작을 보증한다
            TopicPartition latestPartition = null;
            long lastOffset=0;
            long unconsumedTime = 0;

            //커밋되지 않은 레코드를 저장하는 로컬 버퍼 선언
            List<Map<String, Object>> buffer = new ArrayList<>();
            while (true) {
                long start = System.currentTimeMillis();
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(POLL_INTERVAL_MILLIS));
                unconsumedTime += System.currentTimeMillis() - start;

                //TIMEOUT_MILLIS까지 새로운 레코드가 오지 않는다면 지금까지 받아온 레코드로 블록을 만들고 오프셋을 커밋한다
                if(unconsumedTime > TIMEOUT_MILLIS && records.isEmpty()){
                    unconsumedTime = 0;
                    if(buffer.size() > 0){
                        execute(buffer);
                        buffer.clear();
                        log.info("unconsumedTime timeout, consumed uncompleted batch");
                        consumer.commitSync(Collections.singletonMap(latestPartition,new OffsetAndMetadata(lastOffset+1))); // 오프셋 커밋
                    }
                    else{
                        log.info("unconsumedTime timeout, has no data to make block.");
                    }
                } else {
                    if(!records.isEmpty())
                        unconsumedTime = 0;
                    for (TopicPartition partition: records.partitions()) {
                        latestPartition = partition;

                        for (ConsumerRecord<String, Object> record: records.records(partition)) {
                            lastOffset = record.offset();
                            buffer.add((Map<String, Object>) record.value()); // buffer에 담기
                            log.info(String.format("buffer size = %d , offset : %d , value : %s", buffer.size(), record.offset(), record.value()));

                            //poll된 records로 하나씩 채워진 buffer가 1블록 사이즈를 달성했다면 소비하여 블록을 삽입한다
                            if (buffer.size() == Integer.parseInt((String) listerServiceConfigs.get(MIN_BATCH_SIZE))) {
                                execute(buffer);
                                buffer.clear();
                                log.info("consumed 1 batch");
                                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1))); // 오프셋 커밋
                            }
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // 정상적으로 아토믹 불리언이 false이라면 예외를 무시하고 종료한다
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    @Override
    public void execute(Object obj) {
        List<Map<String, Object>> buffer = (List<Map<String,Object>>) obj;

        //TODO : 토픽의 이름에 대문자가 있는 문제로 인하여 하드코딩된 인덱스를 임시로 사용함
        //String index = ((List<String>) listerServiceConfigs.get(TOPICS)).get(0);
        String index = "lee";

        try (Client client = new Client(pbftClientProperties)) {
            try (EsRestClient esRestClient = new EsRestClient(esRestClientConfigs)) {
                try {
                    esRestClient.connectToEs();
                } catch (NoSuchFieldException | EsRestClient.EsSSLException e) {
                    log.error("cannot connect to elasticsearch. error : ", e);
                }

                List<String> hashList = new ArrayList<>();
                for(Map<String, Object> entry : buffer) {
                    String jsonMap = null;
                    try {
                        jsonMap = objectMapper.writeValueAsString(entry);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    hashList.add(Util.hash(jsonMap));
                }
                HashTree hashTree = new HashTree(hashList.toArray(new String[0]));
                int blockNumber = 0;
                try {
                    blockNumber = storeHeaderAndHashToPBFTAndReturnIdx(client, index, hashTree.toString(), hashList);
                } catch (IOException e) {
                    log.error("cannot store block header to Blockchain cluster. continuing next consume... ", e);
                }
                try {
                    esRestClient.bulkInsertDocumentByProcessor(index, index, blockNumber, buffer, false
                        , 1, 1000, 10, ByteSizeUnit.MB, 5);
                } catch (IOException | EsRestClient.EsException | EsRestClient.EsConcurrencyException | InterruptedException e) {
                    //TODO : 삽입이 실패한 경우 되돌릴수 있다면 되돌리기
                    log.error("cannot insert data to elasticsearch. ", e);
                }
                log.info(String.format("Block #%d inserted to Wallaby. block size : %d", blockNumber, buffer.size()));

            } catch (IOException e) {
                log.error("cannot connect to elasticsearch. error : ", e);
            }
        }
    }

    @Override
    public void shutdownConsumer() {
        if(consumer != null) {
            closed.set(true);
            consumer.wakeup();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        asyncExecutionService.runAsConsumerExecutor(this::startConsumer);
    }

    @Override
    public void destroy() throws Exception {
        this.shutdownConsumer();
    }

    private int storeHeaderAndHashToPBFTAndReturnIdx(Client client, String chainName, String root, List<String> hashList) throws IOException {
        //send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
        Operation insertHeaderOp;
        if(isHashListInclude)
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, hashList, root);
        else
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, root);

        RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
        client.request(insertRequestMsg);
        int blockNumber = (int) client.getReply();
        return blockNumber;
    }
}
