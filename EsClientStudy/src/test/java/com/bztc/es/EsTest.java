package com.bztc.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * @author daism
 * @create 2022-12-08 10:20
 * @description es测试
 */
public class EsTest {
    private static ElasticsearchClient client;
    private static ElasticsearchAsyncClient asyncClient;
    private static ElasticsearchTransport transport;

    private static final String INDEX_BZTC = "bztc";

    public static void main(String[] args) throws Exception {
        //初始化连接
        initEsConnection();

        //创建索引
//        createIndex();
//        createIndexByLamda();
        //查询索引
//        queryIndex();
        //删除索引
//        delete();

        //操作文档
//        operateDoc();
        //查询操作
//        searchDoc();
        //异步操作
        asyncOperate();
        //关闭
        closeClient();
    }

    private static void asyncOperate() throws InterruptedException {
        asyncClient.indices().create(
                req -> {
                    req.index("newindex");
                    return req;
                }
        ).whenComplete(
                (resp, error) -> {
                    System.out.println("回调函数");
                    if (resp != null) {
                        System.out.println(resp.acknowledged());
                    } else {
                        error.printStackTrace();
                    }
                });
        Thread.sleep(10*1000);
        System.out.println("主线程操作...");
    }

    private static void searchDoc() throws IOException {
        SearchResponse<Object> search = client.search(req -> {
            req.query(q -> q.match(
                    m -> m.field("name").query("zhangsan")
            ));
            return req;
        }, Object.class);
        System.out.println("查询：" + search);
    }

    private static void operateDoc() throws IOException {
        User user = new User(1, "zhangsan", 30);
        //获取索引对象
        IndexRequest<User> request = new IndexRequest.Builder<User>().index(INDEX_BZTC)
                .id("10001").document(user).build();
        IndexResponse indexResponse = client.index(request);
        System.out.println("操作结果：" + indexResponse);

        List<BulkOperation> bulkOperationList = new ArrayList<>();
        for (int i = 1; i < 6; i++) {
            CreateOperation.Builder builder = new CreateOperation.Builder<User>();
            builder.index(INDEX_BZTC);
            builder.id("200" + i);
            builder.document(new User(2000 + i, "zhangsan" + i, 30));
            CreateOperation<Object> objectCreateOperation = builder.build();
            BulkOperation bulkOperation = new BulkOperation.Builder().create(objectCreateOperation).build();
            bulkOperationList.add(bulkOperation);
        }

        BulkRequest bulkRequest = new BulkRequest.Builder().operations(bulkOperationList).build();
        BulkResponse bulkResponse = client.bulk(bulkRequest);
        System.out.println("操作结果：" + bulkResponse);
    }

    private static void delete() throws IOException {
        //获取索引对象
        ElasticsearchIndicesClient indices = client.indices();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(INDEX_BZTC).build();
        DeleteIndexResponse delete = indices.delete(deleteIndexRequest);
        System.out.println("索引删除成功：" + delete.acknowledged());
    }

    private static void queryIndex() throws IOException {
        //获取索引对象
        ElasticsearchIndicesClient indices = client.indices();
        GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(INDEX_BZTC).build();
        GetIndexResponse getIndexResponse = indices.get(getIndexRequest);
        System.out.println("查询索引:" + getIndexResponse);
    }

    private static void createIndexByLamda() throws IOException {
        //获取索引对象
        ElasticsearchIndicesClient indices = client.indices();
        //判断索引是否存在
        boolean flag = indices.exists(req -> req.index(INDEX_BZTC)).value();
        if (flag) {
            System.out.println("索引已存在");
            return;
        }
        //创建索引
        CreateIndexResponse response = indices.create(req -> req.index(INDEX_BZTC));
        System.out.println("创建索引成功：" + response);
    }

    private static void createIndex() throws IOException {
        //获取索引对象
        ElasticsearchIndicesClient indices = client.indices();
        //判断索引是否存在
        ExistsRequest existsRequest = new ExistsRequest.Builder().index(INDEX_BZTC).build();
        boolean flag = indices.exists(existsRequest).value();
        if (flag) {
            System.out.println("索引已存在");
            return;
        }
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest.Builder().index(INDEX_BZTC).build();
        CreateIndexResponse createIndexResponse = client.indices().create(request);
        System.out.println("创建索引成功：" + createIndexResponse);
    }

    private static void closeClient() throws IOException {
        transport.close();
    }

    private static void initEsConnection() throws Exception {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "CzgP2gTawmgg=*BbT*oX"));
        Path caCertificatePath = Paths.get("/Users/daishuming/IdeaProjects/esStudy/ElasticSearchStudy/EsClientStudy/src/main/java/com/bztc/certs/java-ca.crt");
        CertificateFactory factory =
                CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("10.211.55.5", 9200, "https"))
                .setHttpClientConfigCallback(new
                                                     RestClientBuilder.HttpClientConfigCallback() {
                                                         @Override
                                                         public HttpAsyncClientBuilder customizeHttpClient(
                                                                 HttpAsyncClientBuilder httpClientBuilder) {
                                                             return httpClientBuilder.setSSLContext(sslContext)
                                                                     .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                                                     .setDefaultCredentialsProvider(credentialsProvider);
                                                         }
                                                     });
        RestClient restClient = builder.build();
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);

    }
}
