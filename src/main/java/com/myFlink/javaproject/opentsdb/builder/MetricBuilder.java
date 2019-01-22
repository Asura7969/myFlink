package com.myFlink.javaproject.opentsdb.builder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Builder used to create the JSON to push metrics to KairosDB.
 */
public class MetricBuilder {
	private List<Metric> metrics = new ArrayList<Metric>();
	private transient Gson mapper;

	private MetricBuilder() {
		GsonBuilder builder = new GsonBuilder();
		mapper = builder.create();
	}

	/**
	 * Returns a new metric builder.
	 *
	 * @return metric builder
	 */
	public static MetricBuilder getInstance() {
		return new MetricBuilder();
	}

	/**
	 * Adds a metric to the builder.
	 *
	 * @param metricName
	 *            metric name
	 * @return the new metric
	 */
	public Metric addMetric(String metricName) {
		Metric metric = new Metric(metricName);
		metrics.add(metric);
		return metric;
	}

	/**
	 * Returns a list of metrics added to the builder.
	 *
	 * @return list of metrics
	 */
	public List<Metric> getMetrics() {
		return metrics;
	}

	/**
	 * Returns the JSON string built by the builder. This is the JSON that can
	 * be used by the client add metrics.
	 *
	 * @return JSON
	 * @throws IOException
	 *             if metrics cannot be converted to JSON
	 */
	public String build() throws IOException {
		for (Metric metric : metrics) {
			// verify that there is at least one tag for each metric
			checkState(metric.getTags().size() > 0, metric.getName()
					+ " must contain at least one tag.");
		}
		return mapper.toJson(metrics);
	}
}