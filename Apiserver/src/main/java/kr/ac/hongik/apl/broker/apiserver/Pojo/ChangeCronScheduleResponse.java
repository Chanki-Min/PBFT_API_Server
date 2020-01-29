package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 스케쥴된 블록 검증의 crontab을 변경하는 작업 이후 client에게 전달되는 객체
 */
@AllArgsConstructor
@Getter
public class ChangeCronScheduleResponse {
	/**
	 * result message
	 */
	private String message;
	/**
	 * changed cron
	 */
	private String afterCron;
}
