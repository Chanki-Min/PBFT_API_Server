package kr.ac.hongik.apl.broker.apiserver.Advice;

import kr.ac.hongik.apl.broker.apiserver.Pojo.ExceptionResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

@Slf4j
@ControllerAdvice
public class ExceptionHandlingAdvice {

	/**
	 * ExceptionHandler(Exception.class) 어노테이션을 통하여 controller에서 throw 된 모든 예외를 받아서 최종적으로 client에게 리턴합니다.
	 *
	 * @param e controller에서 throw된 예
	 * @return ExceptionResponse 객체
	 */
	@ExceptionHandler(Exception.class)
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	public ExceptionResponse returnException(Exception e) {
		log.error(e.getMessage());
		return new ExceptionResponse(e, HttpStatus.INTERNAL_SERVER_ERROR);
	}
}
