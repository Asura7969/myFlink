package com.myFlink.javaproject.opentsdb;

import com.myFlink.javaproject.opentsdb.builder.MetricBuilder;
import com.myFlink.javaproject.opentsdb.request.QueryBuilder;
import com.myFlink.javaproject.opentsdb.response.Response;
import com.myFlink.javaproject.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
                                ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
                                          ExpectResponse exceptResponse) throws IOException;

	@Override
	public void shutdown() throws IOException;
}