package com.elasticsearch.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
@Slf4j
@Component
public class ElasticSearchUtil {

    private static RestHighLevelClient client;

    @PostConstruct
    public static void init() {
        client = ElasticSearchUtil.createConnection();
    }

    /**
     * 创建高级链接
     *
     * @return
     */
    public static RestHighLevelClient createConnection() {
        try {
            HttpHost[] host = new HttpHost[]{
                    new HttpHost("localhost", 9200)
            };
            RestClientBuilder builder = RestClient.builder(host);
            // 设置ES 链接密码
            /*
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esProperties.getUserName(), esProperties.getPassword()));
            builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
            */
            // 创建高级搜索链接，请注意改链接使用完成后必须关闭，否则使用一段时间之后将会抛出异常
            client = new RestHighLevelClient(builder);
        } catch (Exception e) {
            client = null;
            log.error("创建es连接失败:" + e.getMessage());
        } finally {
            return client;
        }

    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /**
     * 获取索引
     *
     * @param indexName
     * @throws IOException
     */
    public static GetIndexResponse getIndex(String indexName) {
        if (client == null) {
            init();
        }
        GetIndexResponse getIndexResponse = null;
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info("索引[" + indexName + "]不存在");
            e.printStackTrace();
            return null;
        }
        Map<String, Settings> settingsMap = getIndexResponse.getSettings();
        System.out.println(settingsMap);

        Settings setting = settingsMap.get(indexName);
        System.out.println("setting:" + setting);
        return getIndexResponse;
    }

    /**
     * 添加索引
     *
     * @throws IOException
     */
    public static boolean createIndex(String indexName) {
        if (StringUtils.isEmpty(indexName)) {
            log.error("索引名称为空");
            return false;
        }
        if (client == null) {
            init();
        }
        try {
            //添加索引
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            boolean result = createIndexResponse != null && !StringUtils.isEmpty(createIndexResponse.index());
            if (result) {
                log.info("索引{}删除成功", indexName);
            } else {
                log.error("索引{}删除失败", indexName);
            }
            return result;
        } catch (ElasticsearchStatusException e) {
            log.info("创建es索引[" + indexName + "]已存在:" + e.getMessage());
            return true;
        } catch (Exception e) {
            log.error("创建es索引[" + indexName + "]失败:" + e.getMessage());
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public static boolean deleteIndex(String indexName) throws IOException {
        if (StringUtils.isEmpty(indexName)) {
            log.error("索引名称为空");
        }
        if (client == null) {
            init();
        }
        try {
            //删除索引
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
            boolean resultFlag = delete != null && delete.isAcknowledged();
            if (resultFlag) {
                log.info("索引{}删除成功", indexName);
            } else {
                log.error("索引{}删除失败", indexName);
            }
            return resultFlag;
        } catch (ElasticsearchStatusException e) {
            log.info("删除es索引[" + indexName + "]失败，索引不存在");
            return true;
        } catch (Exception e) {
            log.error("创建es索引[" + indexName + "]失败:" + e.getMessage());
        }
        return false;
    }

    /**
     * 创建或者修改索引内容
     *
     * @param indexName
     * @param id
     * @param sourceJson
     * @return
     */
    public static boolean saveOrUpdateIndexAndData(String indexName, String id, String sourceJson) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(id) || StringUtils.isEmpty(sourceJson)) {
            log.error("index:{}内容创建失败:参数indexName,id或sourceJson缺失", indexName);
            return false;
        }
        IndexResponse response = null;
        try {
            IndexRequest request = new IndexRequest(indexName);
            request.id(id);
            request.source(sourceJson, XContentType.JSON);
            response = client.index(request, RequestOptions.DEFAULT);
            boolean resultFlag = response != null && response.getResult() != null
                    && "CREATED".equals(response.getResult().toString())
                    || "UPDATED".equals(response.getResult().toString());
            if (resultFlag) {
                log.info("index:{}内容创建|修改成功,id:{}", indexName, id);
            } else {
                log.error("index:{}内容创建|修改失败,id:{}", indexName, id);
            }
            return resultFlag;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * id可以不填写，但是会删除索引全部内容
     *
     * @param indexName
     * @param id
     * @return
     */
    public static boolean deleteDataById(String indexName, String id) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(id)) {
            log.error("index:{}内容创建失败:参数id或indexName缺失", indexName);
            return false;
        }
        try {
            DeleteResponse response = client.delete(new DeleteRequest(indexName, id), RequestOptions.DEFAULT);
            return response != null && response.status() == RestStatus.OK;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 根据编号进行查询
     *
     * @param indexName
     * @param id
     * @return
     */
    public static GetResponse queryDataById(String indexName, String id) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(id)) {
            log.error("queryDataById,参数id或indexName缺失", indexName);
            return null;
        }
        GetResponse response = null;
        try {
            GetRequest request = new GetRequest(indexName);
            request.id(id);
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


    /**
     * 普通查询
     *
     * @param
     * @return
     */
    public static SearchResponse queryData(SearchRequest request) {
        if (client == null) {
            init();
        }
        try {
            return client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 分页查询
     *
     * @param request
     * @return
     */
    public static SearchResponse queryPageData(SearchRequest request, Integer pageIndex, Integer pageSize) {
        if (client == null) {
            init();
        }
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            // 从第几条开始
            int from;
            if (pageIndex <= 1) {
                from = 0;
            } else {
                from = pageIndex;
            }
            from = (from * pageSize);
            searchSourceBuilder.from(from);
            //查询多少条
            searchSourceBuilder.size(pageSize);
            //排序
            //searchSourceBuilder.sort();
            request.source(searchSourceBuilder);
            return client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 普通查询全部索引|指定索引中内容
     * 索引名称可以为空
     *
     * @param indexName 索引名称
     * @param myClass   返回的类型
     * @return List
     */
    public static Map<String,Object> queryDataByIndexName(String indexName, Class myClass) {
        if (client == null) {
            init();
        }
        Map<String,Object> results = new HashMap<>();
        SearchRequest request = new SearchRequest(indexName);
        SearchResponse response = queryData(request);
        if (response == null) {
            return null;
        }
        //总记录数
        int totalCount = response.getHits().getHits().length;
        if (totalCount > 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                //包含内容的indexName
                String index = hit.getIndex();
                String dataJson = hit.getSourceAsString();
                results.put(index,JSONObject.parseObject(dataJson, myClass));
            }
        }
        System.out.println("results:" + results);
        return results;
    }

    /**
     * 分页查询全部索引|指定索引中内容
     * @param indexName
     * @param myClass
     * @param pageIndex
     * @param pageSize
     * @return
     */
    public static List queryPageDataByIndexName(String indexName, Class myClass,Integer pageIndex, Integer pageSize) {
        if (client == null) {
            init();
        }
        List results = new ArrayList();
        SearchRequest request = new SearchRequest(indexName);
        SearchResponse response = queryPageData(request,pageIndex,pageSize);
        if (response == null) {
            return null;
        }
        //总记录数
        int totalCount = response.getHits().getHits().length;
        //log.info("总记录数:"+response.getHits().getTotalHits().value);
        log.info("总记录数:"+totalCount);
        if (totalCount > 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                String dataJson = hit.getSourceAsString();
                results.add(JSONObject.parseObject(dataJson, myClass));
            }
        }
        System.out.println("results:" + results);
        return results;
    }

}
