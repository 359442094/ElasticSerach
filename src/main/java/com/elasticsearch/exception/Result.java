package com.elasticsearch.exception;

import lombok.Data;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/3
 */
@Data
public class Result extends RuntimeException {

    private String code;

    private String message;

    public Result(String message, String code) {
        super(code + message);
        this.code = code;
        this.message = message;
    }

}
