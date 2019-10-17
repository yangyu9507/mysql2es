package com.gy.config;

import com.alibaba.fastjson.JSONObject;
import gy.lib.common.util.NumberUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * created by yangyu on 2019-09-17
 */
@Configuration
@ConfigurationProperties(prefix = "es.cluster")
@PropertySource("elastic.properties")
public class ElasticSearchClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private static final int DEFAULT_PORT = 9200;
    private String clusterName;
    private String clusterNodes;
    private Integer maxRetryTimeoutMillis;


    @Bean(name = "client",destroyMethod="close") // //这个close是调用RestHighLevelClient中的close
    @Scope("singleton")
    public RestHighLevelClient client() {
        RestHighLevelClient client = null;
        RestClientBuilder clientBulider = getClientBulider();

        client = new RestHighLevelClient(clientBulider);

//        SniffOnFailureListener sniffOnFailureListener =
//                new SniffOnFailureListener();

        //创建Sniffer 并设置一分钟更新一次，默认为5分钟
//        Sniffer sniffer = Sniffer.builder(clientBulider.build())
//                .setSniffIntervalMillis(60000)
//                .setSniffAfterFailureDelayMillis(30000)
//                .build();
//        sniffOnFailureListener.setSniffer(sniffer);

        return client;
    }

    public static void close(RestHighLevelClient client,Sniffer sniffer) throws ESIoException {
        if (null != client) {
            try {
                sniffer.close();
                client.close();
            } catch (IOException e) {
                throw new ESIoException("RestHighLevelClient Client close exception", e);
            }
        }
    }

    private RestClientBuilder getClientBulider() {
        RestClientBuilder restClientBuilder = null;
        try {

            String[] nodes = StringUtils.split(clusterNodes,",");

            List<Node> nodeList = new ArrayList<>();
            /*restClient 初始化*/
            for (String node : nodes) {
                String[] hostPort = StringUtils.split(node,":");
                String host = null;
                Integer port = null;

                if (hostPort != null && hostPort.length > 0) {
                    host = hostPort[0];
                    port = hostPort.length == 2 ? NumberUtil.toInt(hostPort[1]) : DEFAULT_PORT;

                    Node nodeItem = new Node(new HttpHost(host, port, "http"));
                    nodeList.add(nodeItem);
                }
            }

            restClientBuilder = RestClient.builder(nodeList.toArray(new Node[0]));

            /*RestClientBuilder 在构建 RestClient 实例时可以设置以下的可选配置参数*/
            /*1.设置每个请求都需要发送的默认标头，以防止必须在每个单个请求中指定它们*/
            Header[] defaultHeaders = new Header[]{
                    new BasicHeader("header", "value")
            };
            restClientBuilder.setDefaultHeaders(defaultHeaders);

            /*2.设置在同一请求进行多次尝试时应该遵守的超时时间。默认值为30秒，与默认`socket`超时相同。
            如果自定义设置了`socket`超时，则应该相应地调整最大重试超时。  7.3.2版本已废弃*/

//            restClientBuilder.setMaxRetryTimeoutMillis(10000);

            /*3.设置一个侦听器，该侦听器在每次节点发生故障时得到通知，以防需要采取措施。 启用嗅探失败时在内部使用。。*/
            restClientBuilder.setFailureListener(new RestClient.FailureListener() {
                public void onFailure(HttpHost host) {
                    // TODO 这里是当嗅探器测出某个节点有故障时，如何提醒用户，或有问题后做其他操作。
                    logger.error("node[{}] failed.", JSONObject.toJSONString(host));
                }
            });


            // 设置节点选择器以用于过滤客户端将向其自身发送的请求中的客户端发送的节点。 例如，这对于在启用嗅探时阻止将请求发送到专用主节点很有用。 默认情况下，客户端将请求发送到每个已配置的节点。
//            restClientBuilder.setNodeSelector(NodeSelector.ANY);

            /*4.设置修改默认请求配置的回调（例如：请求超时，认证，或者其他
            设置）。*/
            restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                    return requestConfigBuilder.setSocketTimeout(maxRetryTimeoutMillis);
                }
            });

            /*5.//设置修改 http 客户端配置的回调（例如：ssl 加密通讯，线程IO的配置，或其他任何         设置）*/
            // 简单的身份认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("user", "password"));

            restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    //线程设置
                    httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(10).build());
                    return httpClientBuilder;
                }
            });

        } catch (Exception ex) {
            logger.error("ES节点端口配置错误:{}", ex.getMessage());
        }
        return restClientBuilder;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public Integer getMaxRetryTimeoutMillis() {
        return maxRetryTimeoutMillis;
    }

    public void setMaxRetryTimeoutMillis(Integer maxRetryTimeoutMillis) {
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
    }
}
