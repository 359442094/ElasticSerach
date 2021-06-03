package com.elasticsearch.controller;

import com.elasticsearch.util.ElasticSearchUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/3
 */
@RestController
public class TestController {

    @Autowired
    private ElasticSearchUtil searchUtil;

    @RequestMapping(path = "/test",method = RequestMethod.GET)
    public void test() throws IOException {
        searchUtil.createIndex("");
    }

}
