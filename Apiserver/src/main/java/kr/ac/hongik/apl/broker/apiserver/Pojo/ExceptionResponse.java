package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * 발생한 Exeption을 Rest client에게 전달하기 위하여 Exception을 wrapping 하는 객체입니다
 */
@Getter
public class ExceptionResponse {

	public ExceptionResponse(Exception e, HttpStatus status) {
		this.timestamp = System.currentTimeMillis();
		this.status = status.value();
		this.error = status.getReasonPhrase();
		this.message = e.getMessage();
	}

	private long timestamp;
	private int status;
	private String error;
	private String message;
}
