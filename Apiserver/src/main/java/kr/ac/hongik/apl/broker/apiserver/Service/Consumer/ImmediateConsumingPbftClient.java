package kr.ac.hongik.apl.broker.apiserver.Service.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import kr.ac.hongik.apl.broker.apiserver.Configuration.KafkaConsumerConfiguration;
import kr.ac.hongik.apl.broker.apiserver.Pojo.ConsumerInfo;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
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

import static kr.ac.hongik.apl.broker.apiserver.Configuration.KafkaConsumerConfiguration.*;

@Slf4j
@Service
public class ImmediateConsumingPbftClient implements ConsumingPbftClient {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, Object> consumer = null;

    private ConsumerInfo immediateConsumerInfo = new ConsumerInfo();

    private String newTopicName = null;
    private int newSize = 0;
    private int newTimeout = 0;

    @Resource(name = "consumerConfigs")
    private Map<String, Object> consumerConfigs;
    @Resource(name = "ImmediateClientConfigs")
    private Map<String, Object> immediateConsumerConfigs;
    @Resource(name = "pbftClientProperties")
    private Properties pbftClientProperties;
    @Resource(name = "esRestClientConfigs")
    public HashMap<String, Object> esRestClientConfigs;

    //Async 노테이션의 메소드는 this가 invoke()할 수 없기 때문에 비동기 실행만 시키는 서비스를 주입한다
    private final AsyncExecutionService asyncExecutionService;
    private final ObjectMapper objectMapper;
    private final ConsumerDataService consumerDataService;

    @Autowired
    public ImmediateConsumingPbftClient(AsyncExecutionService asyncExecutionService, ObjectMapper objectMapper, ConsumerDataService consumerDataService) {
        this.asyncExecutionService = asyncExecutionService;
        this.objectMapper = objectMapper;
        this.consumerDataService = consumerDataService;
    }

    @Override
    @Async("consumerThreadPool")
    public ConsumerInfo startConsumer() {
        try {
            //consumerConfigs.replace(ConsumerConfig.GROUP_ID_CONFIG,"lee2");
            log.info("Start ConsumingPbftClientBuffer service");
            consumer = new KafkaConsumer<>(consumerConfigs);
            consumer.subscribe((Collection<String>) immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_TOPICS));
            long lastOffset;
            long unconsumedTime=0;

            //커밋되지 않은 레코드를 저장하는 로컬 버퍼 선언
            List<Map<String, Object>> buffer;
            while (true) {
                // 이 블럭은 제대로 실행되는지 확인하기 위한 코드임
                long start = System.currentTimeMillis();
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis((Long) immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_POLL_INTERVAL_MILLIS)));
                consumerDataService.setData(consumer.subscription().stream().findFirst().get(),
                        (Integer)immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_TIMEOUT_MILLIS));
                unconsumedTime += System.currentTimeMillis() - start;
                if (unconsumedTime > ((int) immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_TIMEOUT_MILLIS))) {
                    unconsumedTime = 0;
                    log.debug("Immediate Client running...");
                }

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, Object> record : partitionRecords) {
                            lastOffset = record.offset();
                            //Immediate Consumer는 각 레코드가 List<Map>인 것을 받아서 각 레코드를 받은 즉시 execute한다
                            buffer = (List<Map<String, Object>>) record.value();
                            execute(buffer);
                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1))); // 오프셋 커밋
                        }
                    }
                } else {
                    continue;
                }
            }
        } catch (WakeupException e) {
            // 정상적으로 아토믹 불리언이 false이라면 예외를 무시하고 종료한다
            if (!closed.get()) {
                immediateConsumerInfo.setError(true);
                immediateConsumerInfo.setException(e);
                // TODO : 설정 플래그 초기화 여부 해결
                throw e;
            }
        } catch (Exception e){
            immediateConsumerInfo.setError(true);
            immediateConsumerInfo.setException(e);
            // TODO : 설정 플래그 초기화 여부 해결
            throw e;
        } finally {
            immediateConsumerInfo.setMinBatchSize(0);
            immediateConsumerInfo.setTopicName(consumer.subscription().toString());
            immediateConsumerInfo.setTimeout((Integer)immediateConsumerConfigs.get(IMMEDIATE_CONSUMER_TIMEOUT_MILLIS));
            // TODO : db에 삽입
            consumerDataService.deleteData(consumer.subscription().toString());
            consumer.close();

            if(immediateConsumerInfo.isSettings()){
                immediateConsumerInfo.setTopicName(newTopicName);
                immediateConsumerInfo.setMinBatchSize(newSize);
                immediateConsumerInfo.setTimeout(newTimeout);
            }
        }
        return immediateConsumerInfo;
    }

    @Override
    public void execute(Object obj) {
        List<Map<String, Object>> buffer = (List<Map<String,Object>>) obj;
        String index = ((List<String>) immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_TOPICS)).get(0);

        try (Client client = new Client(pbftClientProperties)) {
            try (EsRestClient esRestClient = new EsRestClient(esRestClientConfigs)) {
                try {
                    esRestClient.connectToEs();
                } catch (NoSuchFieldException | EsRestClient.EsSSLException e) {
                    log.error("cannot connect to elasticsearch. error : ", e);
                }

                List<String> hashList = new ArrayList<>();
                for(Map<String, Object> entry : buffer) {
                    String jsonMap;
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
        asyncExecutionService.runAsConsumerExecutor(this::startConsumer,immediateConsumerInfo);
    }

    public void destroy() throws Exception {
        immediateConsumerInfo.setShutdown(true);
        this.shutdownConsumer();
    }

    /**
     * 케이스 번호가 0 즉, 설정을 동적으로 변경하고자 하면,
     * 먼저 finally에서 객체 정보를 db에 저장하고,
     * 넘겨받은 인자들을 기존 객체에 쓴 후,
     * 그 객체를 반환 후, 재가동 시 그대로 쓴다.
     *
     * @param topicName
     * @param minBatchSize
     * @param timeout
     * @throws Exception
     */
    public void acceptConsumerSettings(String topicName, int minBatchSize, int timeout) throws Exception {
        immediateConsumerInfo.setSettings(true);
        newTopicName = topicName;
        newSize = minBatchSize;
        newTimeout = timeout;
        this.shutdownConsumer();
    }

    private int storeHeaderAndHashToPBFTAndReturnIdx(Client client, String chainName, String root, List<String> hashList) throws IOException {
        //send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
        Operation insertHeaderOp;
        if( ((boolean) immediateConsumerConfigs.get(KafkaConsumerConfiguration.IMMEDIATE_CONSUMER_IS_HASHLIST_INCLUDE)) )
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, hashList, root);
        else
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, root);

        RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
        client.request(insertRequestMsg);
        int blockNumber = (int) client.getReply();
        return blockNumber;
    }
}
