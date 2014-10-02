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

	public static final String GRAPHITE_HOST = "alooma.metrics.graphite.host";
	public static final String GRAPHITE_PORT = "alooma.metrics.graphite.port";
	public static final String GRAPHITE_OUTPUT_FORMAT = "alooma.metrics.graphite.format";
	public static final String GRAPHITE_OUTPUT_FORMAT_SINGLE = "alooma.metrics.graphite.format_single";

	/*
	 * default values for graphite host and port
	 */
	private String graphiteHost = "localhost";
	private int graphitePort = 2003;
	private String format = String.format("%s.%s.%s.%s.%s.%s", 
			OutputFormatConstructs.SRC_WORKER_HOST.magicSequence,
			OutputFormatConstructs.SRC_WORKER_PORT.magicSequence,
			OutputFormatConstructs.SRC_COMPONENT_ID.magicSequence,
			OutputFormatConstructs.SRC_TASK_ID.magicSequence,
			OutputFormatConstructs.METRIC_NAME.magicSequence,
			OutputFormatConstructs.METRIC_KEY.magicSequence);
	
	private String formatSingle = String.format("%s.%s.%s.%s.%s", 
			OutputFormatConstructs.SRC_WORKER_HOST.magicSequence,
			OutputFormatConstructs.SRC_WORKER_PORT.magicSequence,
			OutputFormatConstructs.SRC_COMPONENT_ID.magicSequence,
			OutputFormatConstructs.SRC_TASK_ID.magicSequence,
			OutputFormatConstructs.METRIC_NAME.magicSequence);
	
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		LOG.trace("preparing grapite metrics consumer");
		String graphiteHostFromConf = (String) stormConf.get(GRAPHITE_HOST);
		String graphitePortFromConf = (String) stormConf.get(GRAPHITE_PORT);
		String formatFromConf = (String) stormConf.get(GRAPHITE_OUTPUT_FORMAT);
		String formatSingleFromConf = (String) stormConf.get(GRAPHITE_OUTPUT_FORMAT_SINGLE);
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
		if(null != formatFromConf) {
			this.format = formatFromConf;
		}
		if(null != formatSingleFromConf) {
			this.formatSingle = formatSingleFromConf;
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
						graphiteWriter.printf("%s %s %d\n", OutputFormatConstructs.formatStringForGraphite(format, taskInfo, p.name, (String) e.getKey()), e.getValue(), taskInfo.timestamp);
					}
				} else if(p.value instanceof Number) {
					graphiteWriter.printf("%s %s %d\n", OutputFormatConstructs.formatStringForGraphite(formatSingle, taskInfo, p.name, null), p.value, taskInfo.timestamp);
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
	
	/***
	 * The graphite output path is determined by these constructs.
	 * The metric value and the timestamp will always be appended to conform with graphite format
	 * The default output string when metric key exists is: "%swh.%swp.%sti.%mn.%mk"
	 * The default output string when metric key does not exist is: "%swh.%swp.%sti.%mn"
	 * Both can be overriden via Storm configuration
	 */
	public enum OutputFormatConstructs {
		SRC_COMPONENT_ID("%sci"),
		SRC_TASK_ID("%sti"),
		SRC_WORKER_HOST("%swh"),
		SRC_WORKER_PORT("%swp"),
		UPDATE_INTERVAL_SEC("%uis"),
		METRIC_NAME("%mn"),
		METRIC_KEY("%mk")
		;
		
		String magicSequence;
		
		OutputFormatConstructs(String magicSequence) {
			this.magicSequence = magicSequence;
		}
		
		/***
		 * Build the formatted string according to the required format
		 * This is not the most efficient implementation, but this occurs only on metric registration, which is not very frequent
		 */
		public static String formatStringForGraphite(String formatString, TaskInfo taskInfo, String metricName, String metricKey) {
			String ret = formatString.replace(SRC_COMPONENT_ID.magicSequence, taskInfo.srcComponentId);
			ret = ret.replace(SRC_TASK_ID.magicSequence, String.valueOf(taskInfo.srcTaskId));
			ret = ret.replace(SRC_WORKER_HOST.magicSequence, taskInfo.srcWorkerHost);
			ret = ret.replace(SRC_WORKER_PORT.magicSequence, String.valueOf(taskInfo.srcWorkerPort));
			ret = ret.replace(UPDATE_INTERVAL_SEC.magicSequence, String.valueOf(taskInfo.updateIntervalSecs));
			ret = ret.replace(METRIC_NAME.magicSequence, metricName);
			if(null != metricKey) {
				ret = ret.replace(METRIC_KEY.magicSequence, metricKey);
			}
			return ret;
		}
	}
}