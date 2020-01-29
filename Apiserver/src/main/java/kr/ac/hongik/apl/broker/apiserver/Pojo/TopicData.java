package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.*;

import java.io.Serializable;
import java.util.List;


@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter @ToString
public class TopicData implements Serializable {
    private String name;
    private boolean internal;
    private List partitions;
}