package com.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.model.BatchRequest;
import com.elasticsearch.model.User;
import com.elasticsearch.util.ElasticSearchUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
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

    private static void queryData() {
        SearchRequest searchRequest = new SearchRequest();
        // 设置查询类型:QUERY_THEN_FETCH DFS_QUERY_THEN_FETCH DEFAULT
        searchRequest.searchType(SearchType.DEFAULT);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询全部
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //&&查询
        //searchSourceBuilder.query(QueryBuilders.termQuery("name","22"));
        /*
        searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:22"));
        searchSourceBuilder.query(QueryBuilders.queryStringQuery("userId:123421"));
        */
        //组合查询
        /**
         * must 相当于 与 & =
         *
         * must not 相当于 非 ~   ！=
         *
         * should 相当于 或  |   or
         *
         * filter  过滤
         * */
        /*searchSourceBuilder.query(QueryBuilders.boolQuery()
                .should(QueryBuilders.wildcardQuery("name","*"+"cj"+"*"))
                .should(QueryBuilders.termQuery("name","22"))
                //过滤之后只显示符合最后一项内容
                .filter(QueryBuilders.termQuery("name","22"))
        );*/
        //in查询
        /*searchSourceBuilder.query(QueryBuilders.boolQuery()
                .should(QueryBuilders.queryStringQuery("name:22"))
                .should(QueryBuilders.queryStringQuery("name:user-cj55"))
        );*/
        //模糊查询
        //searchSourceBuilder.query(QueryBuilders.wildcardQuery("name","*"+"cj"+"*"));

        //最大值查询
        //searchSourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));

        //分组查询
        // searchSourceBuilder.aggregation(AggregationBuilders.terms("ageGroup").field("age"));

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = ElasticSearchUtil.queryData(searchRequest);
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getIndex());
            System.out.println(hit.getSourceAsString());
        }
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
        System.out.println("allIndexData:"+allIndexData);
        System.out.println("size:"+allIndexData.size());*/
        /*allIndexData.stream().forEach(index->{
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

        //普通查询全部索引内容不为空索引|指定索引中内容
        //Map<String,Object> map = ElasticSearchUtil.queryDataByIndexName("", User.class);

        //分页查询全部索引|指定索引中内容
        /*int pageIndex = -1;
        int pageSize = 3;
        List<User> pageList = ElasticSearchUtil.queryPageDataByIndexName("", User.class, pageIndex, pageSize);
        */
        //queryData();

        //查询分词效果
        //ElasticSearchUtil.getAnalyzeToken("user-cj55","中华人民共和国国歌");

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
