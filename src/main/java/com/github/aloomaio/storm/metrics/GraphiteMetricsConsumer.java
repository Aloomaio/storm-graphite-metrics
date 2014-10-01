package com.github.aloomaio.storm.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
 * metric name is taken "as is", and is prepended by the taskInfo.srcWorkerHost, taskInfo.srcWorkerPort, taskInfo.srcTaskId
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
		LOG.trace("preparing grapite metrics consumer");
		String graphiteHostFromConf = (String) stormConf.get(GRAPHITE_HOST_KEY);
		String graphitePortFromConf = (String) stormConf.get(GRAPHITE_PORT_KEY);
		if (null != graphiteHostFromConf) {
			graphiteHost = graphiteHostFromConf;
		}
		if (null != graphitePortFromConf) {
			try {
				graphitePort = Integer.valueOf(graphitePortFromConf);
			} catch(NumberFormatException e) {
				LOG.error(String.format("port must be an Integer, got: %s", graphitePortFromConf));
				throw e;
			}
		}
	}

	public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		try {
			LOG.trace(String.format("Connecting to graphite on %s:%d", graphiteHost, graphitePort));
			Socket socket = new Socket(graphiteHost, graphitePort);
			PrintWriter graphiteWriter = new PrintWriter(socket.getOutputStream(), true);
			LOG.trace(String.format("Graphite connected, got %d datapoints", dataPoints.size()));
			for (DataPoint p : dataPoints) {
				LOG.trace(String.format("Registering data point to graphite: %s, %s. Value type is: %s", p.name, p.value, p.value.getClass().getCanonicalName()));
				//yikes, we must use Run Time Type Information.
				if(p.value instanceof Map) {
					//storm uses raw map without generic types
					Set<Map.Entry> entries = ((Map) p.value).entrySet();
					for(Map.Entry e : entries) {
						graphiteWriter.printf("%s.%s.%s.%s.%s %s %d\n", taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,taskInfo.srcTaskId, p.name, e.getKey(), e.getValue(), taskInfo.timestamp);
					}
				} else if(p.value instanceof Number) {
					graphiteWriter.printf("%s.%s.%s.%s %s %d\n", taskInfo.srcWorkerHost, taskInfo.srcWorkerPort, taskInfo.srcTaskId, p.name, p.value, taskInfo.timestamp);
				} else {
					//(relatively) Silent failure, as all kinds of metrics can be sent here 
					LOG.debug(String.format("Got datapoint with unsupported type, %s", p.value.getClass().getCanonicalName()));
				}          
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