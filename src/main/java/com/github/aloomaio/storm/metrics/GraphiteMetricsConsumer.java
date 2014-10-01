package com.github.aloomaio.storm.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

/***
 * Copyright 2014 alooma.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Yair Weinberger {yair@alooma.io}
 * */

/****
 * This class enable metric transmission from storm to graphite.
 * Graphite connection details can be provided via storm configuration.
 * metric name is taken "as is".
 */
public class GraphiteMetricsConsumer implements IMetricsConsumer {
	public static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricsConsumer.class);

	public static final String GRAPHITE_HOST_KEY = "alooma.metrics.graphite.host";
	public static final String GRAPHITE_PORT_KEY = "alooma.metrics.graphite.port";

	/*
	 * default values for graphite host and port
	 */
	private String graphiteHost = "localhost";
	private int graphitePort = 2003;

	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		String graphiteHostFromConf = (String) stormConf.get(GRAPHITE_HOST_KEY);
		Integer graphitePortFromConf = (Integer) stormConf.get(GRAPHITE_PORT_KEY);
		if (null != graphiteHostFromConf) {
			graphiteHost = graphiteHostFromConf;
		}
		if (null != graphitePortFromConf) {
			graphitePort = graphitePortFromConf.intValue();
		}
	}

	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		long graphiteTimestamp = taskInfo.timestamp / 1000;
		try {
			LOG.trace(String.format("Connecting to graphite on %s:%d", graphiteHost, graphitePort));
			Socket socket = new Socket(graphiteHost, graphitePort);
			PrintWriter graphiteWriter = new PrintWriter(socket.getOutputStream(), true);
			for (DataPoint p : dataPoints) {
				LOG.trace(String.format("Registering data point to graphite: %s, %s", p.name, p.value));
				graphiteWriter.printf("%s %d %d\n", p.name, p.value, graphiteTimestamp);
			}
			graphiteWriter.close();
			socket.close();
		} catch(IOException e) {
			LOG.error("IOException in graphite metrics consumer", e);
			throw new RuntimeException(e);
		}
	}

	public void cleanup() {
		/* do nothing */
	}
}