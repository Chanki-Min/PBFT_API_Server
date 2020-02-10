package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class ConsumerData {
/*    센서인지 공정인지 (그 외)인지, 컨슈머 종류의 이름 제공 */
    public String consumerType;
    public int timeout;
    public int minbatch;

}
