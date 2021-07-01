package com.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.model.BatchRequest;
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

    private static final List<User> users = User.users;

    private void queryData() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
    }

    public static void main(String[] args) throws IOException {
        String indexName = "user-cj1234";
        String id = "userId-12341";
        // 创建连接
        ElasticSearchUtil.createConnection();

        //查询单个索引
        //ElasticSearchUtil.getIndex(indexName);
        //创建索引
        //ElasticSearchUtil.createIndex(indexName);
        //删除索引
        //ElasticSearchUtil.deleteIndex(indexName);

        //创建索引内容
        //testSaveData(indexName,id);

        //批量创建索引内容
        //testBatchSaveIndexAndData();

        //修改索引及内容
        /*boolean data = ElasticSearchUtil.updateIndexAndData(indexName, id, JSONObject.toJSONString(users.get(1)));
        System.out.println(data);*/

        //查询全部索引信息
        /*List<String> allIndexData = ElasticSearchUtil.getCatAllIndexData();
        System.out.println("size:"+allIndexData.size());
        allIndexData.stream().forEach(index->{
            System.out.println(index);
            //删除索引
            try {
                ElasticSearchUtil.deleteIndex(index);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });*/

        //根据编号进行查询
        //ElasticSearchUtil.queryDataById(indexName,id);

        //删除索引内容
        //ElasticSearchUtil.deleteDataById(indexName, id);

        //删除全部索引及内容
        //ElasticSearchUtil.deleteData();

        //普通查询全部索引|指定索引中内容
        //Map<String,Object> map = ElasticSearchUtil.queryDataByIndexName("", User.class);

        //分页查询全部索引|指定索引中内容
        /*int pageIndex = 2;
        int pageSize = 3;
        List<User> pageList = ElasticSearchUtil.queryPageDataByIndexName("", User.class, pageIndex, pageSize);
        */

        ElasticSearchUtil.closeConnection();
    }

    /**
     * 测试创建索引内容
     *
     * @param indexName
     * @param id
     */
    private static void testSaveData(String indexName, String id) {
        for (User user : users) {
            String jsonString = JSONObject.toJSONString(user);
            ElasticSearchUtil.saveIndexAndData(indexName, id, jsonString);
        }
    }

    /**
     * 测试批量创建索引内容
     */
    private static void  testBatchSaveIndexAndData(){
        List<BatchRequest> requests = new ArrayList<>();
        users.stream().forEach(user -> {
            requests.add(new BatchRequest("user-"+user.getName(),"userId-"+user.getUserId(),JSONObject.toJSONString(user)));
        });
        ElasticSearchUtil.batchSaveIndexAndData(requests);
    }

}
