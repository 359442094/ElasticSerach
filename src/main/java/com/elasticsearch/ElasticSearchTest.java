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
import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/2
 */
public class ElasticSearchTest {

    private void queryData() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
    }

    public static void main(String[] args) throws IOException {
        String indexName = "";
        String id = "1234";
        // 创建连接
        ElasticSearchUtil.createConnection();

        //查询单个索引
        //ElasticSearchUtil.getIndex(indexName);
        //创建索引
        //ElasticSearchUtil.createIndex(indexName);
        //删除索引
        //ElasticSearchUtil.deleteIndex(indexName);

        //创建或者修改索引内容
        //testSaveOrUpdateData(indexName,id);

        //根据编号进行查询
        //ElasticSearchUtil.queryDataById(indexName,id);

        //删除索引内容
        //ElasticSearchUtil.deleteDataById(indexName, id);

        //删除全部索引及内容
        //ElasticSearchUtil.deleteData();

        //普通查询全部索引|指定索引中内容
        Map<String,Object> map = ElasticSearchUtil.queryDataByIndexName(indexName, User.class);
        map.entrySet().forEach(entry->{
            String key = entry.getKey();
            try {
                ElasticSearchUtil.deleteIndex(key);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        //分页查询全部索引|指定索引中内容
        /*int pageIndex = 1;
        int pageSize = 5;
        List<User> pageList = ElasticSearchUtil.queryPageDataByIndexName(indexName, User.class, pageIndex, pageSize);*/


        ElasticSearchUtil.closeConnection();
    }

    /**
     * 创建或者修改索引内容
     *
     * @param indexName
     * @param id
     */
    private static void testSaveOrUpdateData(String indexName, String id) {
        List<User> users = User.users;
        for (User user : users) {
            String jsonString = JSONObject.toJSONString(user);
            ElasticSearchUtil.saveOrUpdateIndexAndData(indexName, id, jsonString);
        }
    }

}
