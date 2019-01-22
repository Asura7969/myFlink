package com.myFlink.javaproject.opentsdb;

import com.myFlink.javaproject.opentsdb.builder.MetricBuilder;
import com.myFlink.javaproject.opentsdb.request.QueryBuilder;
import com.myFlink.javaproject.opentsdb.response.Response;
import com.myFlink.javaproject.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface Client {

	String PUT_POST_API = "/api/put";

    String QUERY_POST_API = "/api/query";

	/**
	 * Sends metrics from the builder to the KairosDB server.
	 *
	 * @param builder
	 *            metrics builder
	 * @return response from the server
	 * @throws IOException
	 *             problem occurred sending to the server
	 */
	Response pushMetrics(MetricBuilder builder) throws IOException;

	SimpleHttpResponse pushQueries(QueryBuilder builder) throws IOException;

	void shutdown() throws IOException;
}