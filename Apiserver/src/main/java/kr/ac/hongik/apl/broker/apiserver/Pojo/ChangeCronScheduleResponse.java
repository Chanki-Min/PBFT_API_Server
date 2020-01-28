package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ChangeCronScheduleResponse {
	private String message;
	private String afterCron;
}
