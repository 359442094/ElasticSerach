package com.elasticsearch.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.net.ConnectException;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/3
 */
@Slf4j
@RestControllerAdvice
public class ExceptionAdvice {

    @ResponseBody
    @ExceptionHandler(value = ConnectException.class)
    public Result connectException(ConnectException e) {
        log.error("ElasticSearch连接失败:" + e.getMessage());
        return new Result("400", e.getMessage());
    }

    @ResponseBody
    @ExceptionHandler(value = Exception.class)
    public Result exceptionAll(Exception e) {
        log.error("2未知异常！原因是:" + e);
        return new Result("400", e.getMessage());
    }

}
