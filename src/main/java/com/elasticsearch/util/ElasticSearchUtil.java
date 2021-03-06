package com.elasticsearch.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.model.BatchRequest;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

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
     * ??????????????????
     *
     * @return
     */
    public static RestHighLevelClient createConnection() {
        try {
            HttpHost[] host = new HttpHost[]{
                    new HttpHost("localhost", 9200)
            };
            RestClientBuilder builder = RestClient.builder(host);
            // ??????ES ????????????
            /*
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esProperties.getUserName(), esProperties.getPassword()));
            builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
            */
            // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
            client = new RestHighLevelClient(builder);
        } catch (Exception e) {
            client = null;
            log.error("??????es????????????:" + e.getMessage());
        } finally {
            return client;
        }

    }

    /**
     * ??????es??????
     * @param url
     * @return
     * @throws IOException
     */
    public static Response sendEsConnection(String url) throws IOException {
        Request request = new Request(HttpMethod.GET.toString(), url);
        return client.getLowLevelClient().performRequest(request);
    }

    /**
     * ????????????????????????
     * @return
     */
    public static List<String> getCatAllIndexData(){
        List<String> results = new ArrayList<>();
        try {
            Response response = sendEsConnection("/_cat/indices?format=json");
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String content = EntityUtils.toString(response.getEntity());
                List<JSONObject> jsonObjects = JSONObject.parseArray(content, JSONObject.class);
                if(jsonObjects!=null){
                    results = jsonObjects.stream().filter(jsonObject -> jsonObject!=null)
                            .map(jsonObject -> jsonObject.getString("index")).collect(Collectors.toList());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /**
     * ????????????
     *
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /**
     * ????????????
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
        } catch (Exception e) {
            log.error("??????[" + indexName + "]?????????:{}",e.getMessage());
            return null;
        }
        Map<String, Settings> settingsMap = getIndexResponse.getSettings();
        System.out.println(settingsMap);

        Settings setting = settingsMap.get(indexName);
        System.out.println("setting:" + setting);
        return getIndexResponse;
    }

    /**
     * ????????????
     *
     * @throws IOException
     */
    public static boolean createIndex(String indexName) {
        if (StringUtils.isEmpty(indexName)) {
            log.error("??????????????????");
            return false;
        }
        if (client == null) {
            init();
        }
        try {
            //????????????
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            boolean result = createIndexResponse != null && !StringUtils.isEmpty(createIndexResponse.index());
            if (result) {
                log.info("??????{}????????????", indexName);
            } else {
                log.error("??????{}????????????", indexName);
            }
            return result;
        } catch (ElasticsearchStatusException e) {
            log.info("??????es??????[" + indexName + "]?????????:" + e.getMessage());
            return true;
        } catch (Exception e) {
            log.error("??????es??????[" + indexName + "]??????:" + e.getMessage());
        }
        return false;
    }

    /**
     * ????????????
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public static boolean deleteIndex(String indexName) throws IOException {
        if (StringUtils.isEmpty(indexName)) {
            log.error("??????????????????");
        }
        if (client == null) {
            init();
        }
        try {
            //????????????
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
            boolean resultFlag = delete != null && delete.isAcknowledged();
            if (resultFlag) {
                log.info("??????{}????????????", indexName);
            } else {
                log.error("??????{}????????????", indexName);
            }
            return resultFlag;
        } catch (ElasticsearchStatusException e) {
            log.info("??????es??????[" + indexName + "]????????????????????????");
            return true;
        } catch (Exception e) {
            log.error("??????es??????[" + indexName + "]??????:" + e.getMessage());
        }
        return false;
    }

    /**
     * ?????????????????????
     *
     * @param indexName
     * @param id
     * @param sourceJson
     * @return
     */
    public static boolean saveIndexAndData(String indexName, String id, String sourceJson) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(id) || StringUtils.isEmpty(sourceJson)) {
            log.error("index:{}??????????????????:??????indexName,id???sourceJson??????", indexName);
            return false;
        }
        IndexResponse response = null;
        try {
            IndexRequest request = new IndexRequest(indexName);
            request.id(id);
            request.source(sourceJson, XContentType.JSON);
            response = client.index(request, RequestOptions.DEFAULT);
            boolean resultFlag = response != null && response.getResult() != null
                    && "CREATED".equals(response.getResult().toString());
            if (resultFlag) {
                log.info("index:{}??????????????????,id:{}", indexName, id);
            } else {
                log.error("index:{}??????????????????,id:{}", indexName, id);
            }
            return resultFlag;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * ???????????????????????????
     *
     * @throws IOException
     */
    public static boolean batchSaveIndexAndData(List<BatchRequest> requests) {
        if (requests == null || requests != null && requests.size()<=0) {
            log.error("request????????????");
            return false;
        }
        BulkRequest bulkRequest = new BulkRequest();
        requests.stream().filter(request->request!=null).forEach(request->{
            bulkRequest.add(new IndexRequest().index(request.getIndexName()).id(request.getId()).source(request.getSource(), XContentType.JSON));
        });
        BulkResponse responses = batch(bulkRequest);
        boolean resultFlag = responses != null && responses.getItems() != null && responses.getItems().length == requests.size();
        if (resultFlag) {
            log.info("????????????????????????");
        } else {
            log.error("????????????????????????");
        }
        return resultFlag;
    }

    /**
     * ?????????????????????
     * @param indexName
     * @param id
     * @param sourceJson
     * @return
     */
    public static boolean updateIndexAndData(String indexName, String id, String sourceJson){
        if(client == null){
            init();
        }
        if(StringUtils.isEmpty(indexName)||StringUtils.isEmpty(id)||StringUtils.isEmpty(sourceJson)){
            log.error("index:{}??????????????????:??????indexName,id???sourceJson??????", indexName);
            return false;
        }
        try {
            GetIndexResponse indexResponse = getIndex(indexName);
            if(indexResponse == null){
                log.error("??????:{}?????????",indexName);
                return false;
            }
            //????????????????????????????????????????????????????????????????????????
            deleteIndex(indexName);

            UpdateRequest request = new UpdateRequest().index(indexName).id(id).doc(sourceJson,XContentType.JSON);
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            return response.getResult() == DocWriteResponse.Result.UPDATED || response.getResult() == DocWriteResponse.Result.CREATED ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * ????????????
     * @param request
     * @return
     */
    public static BulkResponse batch(BulkRequest request){
        try {
            return client.bulk(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * id???????????????????????????????????????????????????
     *
     * @param indexName
     * @param id
     * @return
     */
    public static boolean deleteDataById(String indexName, String id) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(indexName) ) {
            log.error("index:{}??????????????????:??????indexName??????", indexName);
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
     * ????????????????????????
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
            log.error("queryDataById,??????id???indexName??????", indexName);
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
     * ????????????
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
     * ????????????
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
            // ??????????????????
            int from = pageIndex <= 1 ? 0 : pageIndex * pageSize - pageSize;
            int max = (pageIndex <= 0 ? 1 * pageSize : pageIndex * pageSize);
            searchSourceBuilder.from(from);
            //???????????????
            searchSourceBuilder.size(max);

            //??????????????????
            searchSourceBuilder.trackTotalHits(true);

            System.out.println("this:"+from+"-"+max);
            //??????
            //searchSourceBuilder.sort();
            request.source(searchSourceBuilder);
            return client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * ????????????????????????|?????????????????????
     * ????????????????????????
     *
     * @param indexName ????????????
     * @param myClass   ???????????????
     * @return List
     */
    public static Map<String,Object> queryDataByIndexName(String indexName, Class myClass) {
        Map<String,Object> resultMap = new HashMap<>();
        if(!StringUtils.isEmpty(indexName)){
            GetIndexResponse indexResponse = getIndex(indexName);
            if(indexResponse == null){
                return resultMap;
            }
        }
        if (client == null) {
            init();
        }
        SearchRequest request = new SearchRequest(indexName);
        SearchResponse response = queryData(request);
        if (response == null || response.status() == RestStatus.NOT_FOUND) {
            return resultMap;
        }
        //????????????
        int totalCount = response.getHits().getHits().length;
        if (totalCount > 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                //???????????????indexName
                String index = hit.getIndex();
                String dataJson = hit.getSourceAsString();
                resultMap.put(index,JSONObject.parseObject(dataJson, myClass));
            }
        }
        System.out.println("resultMap:" + resultMap.size());
        return resultMap;
    }

    /**
     * ????????????????????????|?????????????????????
     * @param indexName
     * @param myClass
     * @param pageIndex
     * @param pageSize
     * @return
     */
    public static List queryPageDataByIndexName(String indexName, Class myClass,Integer pageIndex, Integer pageSize) {
        List results = new ArrayList();
        if(!StringUtils.isEmpty(indexName)){
            GetIndexResponse indexResponse = getIndex(indexName);
            if(indexResponse == null){
                return results;
            }
        }
        if (client == null) {
            init();
        }
        SearchRequest request = new SearchRequest(indexName);
        SearchResponse response = queryPageData(request,pageIndex,pageSize);
        if (response == null || response.status() == RestStatus.NOT_FOUND) {
            log.error("???????????????");
            return results;
        }

        //????????????
        int totalCount = Integer.valueOf(response.getHits().getTotalHits().value+"");
        //??????????????????
        int thisPageTotalCount = response.getHits().getHits().length;
        //????????????
        int pageCount = totalCount % pageSize == 0 ? totalCount / pageSize : totalCount / pageSize + 1 ;
        log.info("????????????:"+totalCount);
        log.info("?????????:"+pageCount);
        log.info("??????????????????:"+thisPageTotalCount);
        log.info("????????????:"+pageIndex);
        if (thisPageTotalCount > 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                String dataJson = hit.getSourceAsString();
                results.add(JSONObject.parseObject(dataJson, myClass));
            }
        }
        System.out.println("results:" + results);
        return results;
    }

    /**
     * ??????
     * ??????????????????elasticSearch?????????ik??????
     * @throws IOException
     */
    public static List<String> getAnalyzeToken(String indexName,String text) throws IOException {
        //????????????????????????????????????
        /**
         * analyze=ik_max_word ?????????
         * analyze-ik_smart ?????????
         * analyze:standard || ik_max_word
         * */
        if(getIndex(indexName) == null){
            return new ArrayList<>();
        }
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(indexName, "ik_max_word", text);
        AnalyzeResponse response = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
        tokens.stream().forEach(token -> {
            System.out.println(token.getTerm());
        });
        return tokens.stream().map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    }

}
