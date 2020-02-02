package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.common.errors.WakeupException;

@Getter
@Setter
@ToString
public class ConsumerInfo {
    public String topicName;
    public int timeout;
    public int minBatchSize;
    public boolean error = false;
    public boolean settings = false;
    public boolean shutdown = false;

    public Exception exception;
}
