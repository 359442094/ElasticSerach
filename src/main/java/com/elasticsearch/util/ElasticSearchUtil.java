package com.elasticsearch.util;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/6/2
 */
public class ElasticSearchUtil {

    /**
     * 获取高级链接
     *
     * @return
     */
    public static RestHighLevelClient getHighClient() {
        //  getHost() 为获取链接集群的地址
        HttpHost host = new HttpHost("localhost", 9200);
        RestClientBuilder builder = RestClient.builder(host);
        // 设置ES 链接密码
        /*
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esProperties.getUserName(), esProperties.getPassword()));
        builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
        */
        // 创建高级搜索链接，请注意改链接使用完成后必须关闭，否则使用一段时间之后将会抛出异常
        return new RestHighLevelClient(builder);
    }

}
