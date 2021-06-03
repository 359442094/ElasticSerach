package com.elasticsearch.test;

import com.elasticsearch.exception.ExceptionAdvice;
import com.elasticsearch.util.ElasticSearchUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.IOException;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/3
 */
@Slf4j
@RunWith(value = SpringRunner.class)
@SpringBootTest
@ComponentScan(basePackages = {
        "com.elasticsearch.*"
})
public class ElasticSearchTest {

    @Autowired
    private ElasticSearchUtil elasticSearchUtil;

    @Test
    public void test() throws IOException {

        elasticSearchUtil.createIndex("");

    }

}
