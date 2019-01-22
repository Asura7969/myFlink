package com.myFlink.java.project.opentsdb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.myFlink.java.project.opentsdb.builder.MetricBuilder;
import com.myFlink.java.project.opentsdb.request.QueryBuilder;
import com.myFlink.java.project.opentsdb.response.ErrorDetail;
import com.myFlink.java.project.opentsdb.response.Response;
import com.myFlink.java.project.opentsdb.response.SimpleHttpResponse;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * HTTP implementation of a client.
 */
public class HttpClientImpl implements HttpClient, Serializable {

    private static Logger logger = LoggerFactory.getLogger(HttpClientImpl.class);

    private String serviceUrl;

    private Gson mapper;

//    private PoolingHttpClient httpClient = new PoolingHttpClient();
    private PoolingHttpClient httpClient = PoolingHttpClient.getInstance();

    public HttpClientImpl(String serviceUrl) {
        this.serviceUrl = serviceUrl;

        GsonBuilder builder = new GsonBuilder();
        mapper = builder.create();
    }

    @Override
    public Response pushMetrics(MetricBuilder builder) throws IOException {
        return pushMetrics(builder, ExpectResponse.STATUS_CODE);

    }

    @Override
    public Response pushMetrics(MetricBuilder builder,
                                ExpectResponse expectResponse) throws IOException {
        checkNotNull(builder);

        // TODO 错误处理，比如IOException或者failed>0，写到队列或者文件后续重试。
        SimpleHttpResponse response = httpClient
                .doPost(buildUrl(serviceUrl, PUT_POST_API, expectResponse),
                        builder.build());

        return getResponse(response);
    }

    @Override
    public SimpleHttpResponse pushQueries(QueryBuilder builder) throws IOException {
        return pushQueries(builder, ExpectResponse.STATUS_CODE);

    }

    @Override
    public SimpleHttpResponse pushQueries(QueryBuilder builder,
                                          ExpectResponse expectResponse) throws IOException {
        checkNotNull(builder);

        // TODO 错误处理，比如IOException或者failed>0，写到队列或者文件后续重试。
        SimpleHttpResponse response = httpClient
                .doPost(buildUrl(serviceUrl, QUERY_POST_API, expectResponse),
                        builder.build());

        return response;
    }

    @Override
    public void shutdown() throws IOException {
        httpClient.shutdown();
    }

    private String buildUrl(String serviceUrl, String postApiEndPoint,
                            ExpectResponse expectResponse) {
        String url = serviceUrl + postApiEndPoint;

        switch (expectResponse) {
            case SUMMARY:
                url += "?summary";
                break;
            case DETAIL:
                url += "?details";
                break;
            default:
                break;
        }
        return url;
    }

    private Response getResponse(SimpleHttpResponse httpResponse) {
        Response response = new Response(httpResponse.getStatusCode());
        String content = httpResponse.getContent();
        if (StringUtils.isNotEmpty(content)) {
            if (response.isSuccess()) {
                ErrorDetail errorDetail = mapper.fromJson(content,
                        ErrorDetail.class);
                response.setErrorDetail(errorDetail);
            } else {
                logger.error("request failed!" + httpResponse);
            }
        }
        return response;
    }
}