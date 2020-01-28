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

	@ExceptionHandler(Exception.class)
	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	public ExceptionResponse returnException(Exception e) {
		log.error(e.getMessage());
		return new ExceptionResponse(e, HttpStatus.INTERNAL_SERVER_ERROR);
	}
}
