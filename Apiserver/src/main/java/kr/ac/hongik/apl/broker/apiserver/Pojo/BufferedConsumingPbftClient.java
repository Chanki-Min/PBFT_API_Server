package kr.ac.hongik.apl.broker.apiserver.Pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import kr.ac.hongik.apl.broker.apiserver.Service.Asnyc.AsyncExecutionService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumerDataService;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.ConsumingPbftClient;
import kr.ac.hongik.apl.broker.apiserver.Service.Consumer.SendBlockInsertionAckService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.common.unit.ByteSizeUnit;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class BufferedConsumingPbftClient implements ConsumingPbftClient {
    public static final String BUFFERED_CONSUMER_TOPICS = "kafka.listener.service.topic";
    public static final String BUFFERED_CONSUMER_MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
    public static final String BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE = "kafka.listener.service.isHashListInclude";
    public static final String BUFFERED_CONSUMER_TIMEOUT_MILLIS = "kafka.listener.service.timeout.millis";
    public static final String BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS = "kafka.listener.service.poll.interval.millis";



    private final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaConsumer<String, Object> consumer = null;

    private ConsumerInfo bufferedConsumerInfo = new ConsumerInfo();

    private String newTopicName = null;
    private int newSize = 0;
    private int newTimeout = 0;

    private Map<String, Object> consumerConfigs;
    private Map<String, Object> bufferedClientConfigs;
    private Properties pbftClientProperties;
    public HashMap<String, Object> esRestClientConfigs;

    //Async 노테이션의 메소드는 this가 invoke()할 수 없기 때문에 비동기 실행만 시키는 서비스를 주입한다
    private final AsyncExecutionService asyncExecutionService;
    private final ObjectMapper objectMapper;
    private final ConsumerDataService consumerDataService;
    private final SendBlockInsertionAckService sendBlockInsertionAckService;

    public BufferedConsumingPbftClient(Map<String, Object> consumerConfigs, Map<String, Object> bufferedClientConfigs,
                                       Properties pbftClientProperties, HashMap<String, Object> esRestClientConfigs,
                                       AsyncExecutionService asyncExecutionService, ObjectMapper objectMapper,
                                       ConsumerDataService consumerDataService, SendBlockInsertionAckService sendBlockInsertionAckService)
    {

        this.consumerConfigs = consumerConfigs;
        this.bufferedClientConfigs = bufferedClientConfigs;
        this.pbftClientProperties = pbftClientProperties;
        this.esRestClientConfigs = esRestClientConfigs;
        this.asyncExecutionService = asyncExecutionService;
        this.objectMapper = objectMapper;
        this.consumerDataService = consumerDataService;
        this.sendBlockInsertionAckService = sendBlockInsertionAckService;
    }

    @Override
    public ConsumerInfo startConsumer() {
        try {
            log.info("Start ConsumingPbftClientBuffer service");
            consumer = new KafkaConsumer<>(consumerConfigs);
            consumer.subscribe((Collection<String>) bufferedClientConfigs.get(BUFFERED_CONSUMER_TOPICS));

            //TODO : 이 컨슈머는 각 토픽당 파티션이 1개라는 가정 하에 동작을 보증한다
            TopicPartition latestPartition = null;
            long lastOffset=0;
            long unconsumedTime = 0;

            //커밋되지 않은 레코드를 저장하는 로컬 버퍼 선언
            List<Map<String, Object>> buffer = new ArrayList<>();
            while (true) {
                long start = System.currentTimeMillis();
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis((long) bufferedClientConfigs.get(BUFFERED_CONSUMER_POLL_INTERVAL_MILLIS)));
                consumerDataService.setData(consumer.subscription().stream().findFirst().get(),
                        (int) bufferedClientConfigs.get(BUFFERED_CONSUMER_TIMEOUT_MILLIS),
                        (int) bufferedClientConfigs.get(BUFFERED_CONSUMER_MIN_BATCH_SIZE));
                unconsumedTime += System.currentTimeMillis() - start;
                //TIMEOUT_MILLIS까지 새로운 레코드가 오지 않는다면 지금까지 받아온 레코드로 블록을 만들고 오프셋을 커밋한다
                if(unconsumedTime > ((int) bufferedClientConfigs.get(BUFFERED_CONSUMER_TIMEOUT_MILLIS)) && records.isEmpty()){
                    unconsumedTime = 0;
                    if(buffer.size() > 0){
                        execute(buffer);
                        buffer.clear();
                        log.debug("unconsumedTime timeout, consumed uncompleted batch");
                        consumer.commitSync(Collections.singletonMap(latestPartition,new OffsetAndMetadata(lastOffset+1))); // 오프셋 커밋
                    }
                    else{
                        log.debug("unconsumedTime timeout, has no data to make block.");
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
                            if (buffer.size() == (int) bufferedClientConfigs.get(BUFFERED_CONSUMER_MIN_BATCH_SIZE)) {
                                execute(buffer);
                                //마지막 데이터로 Ack를 보낸다
                                Map<String, Object> last = buffer.get(buffer.size()-1);
                                sendBlockInsertionAckService.sendLastData(last);
                                buffer.clear();
                                log.debug("consumed 1 batch");
                                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1))); // 오프셋 커밋
                            }
                        }
                    }
                }
//                this.shutdownConsumer();
            }
        } catch (WakeupException e) {
            // 정상적으로 아토믹 불리언이 false이라면 예외를 무시하고 종료한다
            if (!closed.get()) {
                bufferedConsumerInfo.setError(true);
                bufferedConsumerInfo.setException(e);
                throw e;
            }
        } catch (Exception e){
            bufferedConsumerInfo.setError(true);
            bufferedConsumerInfo.setException(e);
            throw e;
        } finally {
            bufferedConsumerInfo.setTopicName(consumer.subscription().toString());
            bufferedConsumerInfo.setMinBatchSize((Integer)bufferedClientConfigs.get(BUFFERED_CONSUMER_MIN_BATCH_SIZE));
            bufferedConsumerInfo.setTimeout((Integer)bufferedClientConfigs.get(BUFFERED_CONSUMER_TIMEOUT_MILLIS));
            // TODO : db에 삽입
            consumerDataService.deleteData(consumer.subscription().toString());
//            구독하는 토픽은 컨슈머당 1개뿐이라 가정. 그대로 스트링화 하였음.
            consumer.close();
            if(bufferedConsumerInfo.isSettings()){
                bufferedConsumerInfo.setTopicName(newTopicName);
                bufferedConsumerInfo.setMinBatchSize(newSize);
                bufferedConsumerInfo.setTimeout(newTimeout);
            }
        }
        return bufferedConsumerInfo;
    }

    @Override
    public void execute(Object obj) {
        List<Map<String, Object>> buffer = (List<Map<String,Object>>) obj;

        String index = ((List<String>) bufferedClientConfigs.get(BUFFERED_CONSUMER_TOPICS)).get(0);
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
        asyncExecutionService.runAsConsumerExecutor(this::startConsumer,bufferedConsumerInfo);
    }

    @Override
    public void destroy() throws Exception {
        bufferedConsumerInfo.setShutdown(true);
        this.shutdownConsumer();
    }

    /**
     * 케이스 번호가 0 즉, 설정을 동적으로 변경하고자 하면,
     * 먼저 finally에서 객체 정보를 db에 저장하고, ( 고민 해봐야함 )
     * 넘겨받은 인자들을 기존 객체에 쓴 후,
     * 그 객체를 반환 후, 재가동 시 그대로 쓴다.
     *
     * @param topicName
     * @param minBatchSize
     * @param timeout
     * @throws Exception
     */
    public void acceptConsumerSettings(String topicName, int minBatchSize, int timeout) throws Exception {
        bufferedConsumerInfo.setSettings(true);
        newTopicName = topicName;
        newSize = minBatchSize;
        newTimeout = timeout;
        this.shutdownConsumer();
    }

    private int storeHeaderAndHashToPBFTAndReturnIdx(Client client, String chainName, String root, List<String> hashList) throws IOException {
        //send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
        Operation insertHeaderOp;

        if((boolean) bufferedClientConfigs.get(BUFFERED_CONSUMER_IS_HASHLIST_INCLUDE))
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, hashList, root);
        else
            insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), chainName, root);

        RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
        client.request(insertRequestMsg);
        int blockNumber = (int) client.getReply();
        return blockNumber;
    }
}