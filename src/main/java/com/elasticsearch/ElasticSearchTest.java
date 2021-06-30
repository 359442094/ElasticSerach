package com.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.model.User;
import com.elasticsearch.util.ElasticSearchUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/2
 */
public class ElasticSearchTest {

    private void search() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
    }

    public static void main(String[] args) throws IOException {
        String indexName = "test111";
        // 创建连接
        //ElasticSearchUtil.createConnection();

        //查询单个索引
        //ElasticSearchUtil.getIndex(indexName);
        //创建索引
        //ElasticSearchUtil.createIndex(indexName);
        //删除索引
        //ElasticSearchUtil.deleteIndex(indexName);

        //创建索引关联的内容
        testSaveOrUpdateData(indexName);

    }

    private static void testSaveOrUpdateData(String indexName) {
        List<User> users = User.users;
        String id = "1234";
        String jsonString = JSONObject.toJSONString(users.get(0));
        ElasticSearchUtil.saveOrUpdateIndexAndData(indexName, id, jsonString);
    }

}
