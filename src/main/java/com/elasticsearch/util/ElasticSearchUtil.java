package com.elasticsearch.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.*;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

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
        client = ElasticSearchUtil.getHighClient();
    }

    /**
     * 获取高级链接
     *
     * @return
     */
    public static RestHighLevelClient getHighClient() {
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
            return createIndexResponse != null && !StringUtils.isEmpty(createIndexResponse.index());
        } catch (ElasticsearchStatusException e) {
            log.info("创建es索引[" + indexName + "]已存在:" + e.getMessage());
            return true;
        } catch (Exception e) {
            log.error("创建es索引[" + indexName + "]失败:" + e.getMessage());
        }
        return false;
    }

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
            return delete != null && delete.isAcknowledged();
        } catch (ElasticsearchStatusException e) {
            log.info("删除es索引[" + indexName + "]失败，索引不存在");
            return true;
        } catch (Exception e) {
            log.error("创建es索引[" + indexName + "]失败:" + e.getMessage());
        }
        return false;
    }

}
