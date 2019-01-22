package com.myFlink.java.project.opentsdb;

import com.myFlink.java.project.opentsdb.builder.MetricBuilder;
import com.myFlink.java.project.opentsdb.request.QueryBuilder;
import com.myFlink.java.project.opentsdb.response.Response;
import com.myFlink.java.project.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
                                ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
                                          ExpectResponse exceptResponse) throws IOException;

	@Override
	public void shutdown() throws IOException;
}