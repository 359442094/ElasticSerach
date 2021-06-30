package com.elasticsearch.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
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
     * 创建或者修改索引内容
     * @param indexName
     * @param id
     * @param sourceJson
     * @return
     */
    public static boolean saveOrUpdateIndexAndData(String indexName, String id, String sourceJson) {
        if (client == null) {
            init();
        }
        if (StringUtils.isEmpty(id) || StringUtils.isEmpty(sourceJson)) {
            log.error("索引内容{}创建失败:参数id或sourceJson缺失", indexName);
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
                log.info("索引内容{}创建|修改成功", indexName);
            } else {
                log.error("索引内容{}创建|修改失败", indexName);
            }
            return resultFlag;
        } catch (IOException e) {
            e.printStackTrace();
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


}
