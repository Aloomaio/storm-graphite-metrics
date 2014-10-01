package com.github.aloomaio.storm.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
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
	private ObjectMapper mapper = new ObjectMapper();

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

	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		long graphiteTimestamp = taskInfo.timestamp / 1000;
		try {
			LOG.trace(String.format("Connecting to graphite on %s:%d", graphiteHost, graphitePort));
			Socket socket = new Socket(graphiteHost, graphitePort);
			PrintWriter graphiteWriter = new PrintWriter(socket.getOutputStream(), true);
			LOG.trace(String.format("Graphite connected, got %d datapoints", dataPoints.size()));
			for (DataPoint p : dataPoints) {
				LOG.trace(String.format("Registering data point to graphite: %s, %s. Value type is: %s", p.name, p.value, p.value.getClass().getCanonicalName()));
				if(p.value instanceof Map) {
					Set<Map.Entry> entries = ((Map) p.value).entrySet();
					for(Map.Entry e : entries) {
						LOG.trace(String.format("Registering a value inside a datapoint map to graphite: %s, %s. Value type is: %s", e.getKey(), e.getValue(), e.getValue().getClass().getCanonicalName()));
						graphiteWriter.printf("%s.%s.%s.%s %s %d\n", taskInfo.srcWorkerHost, taskInfo.srcWorkerPort, p.name, e.getKey(), e.getValue(), graphiteTimestamp);
					}
				}
				/*
	            JsonFactory factory = mapper.getJsonFactory();
	            JsonParser jp;
	            try {
	            	jp = factory.createJsonParser((String) p.value);
	                JsonNode actualObj = mapper.readTree(jp);
	                Iterator<Entry<String, JsonNode>> itr = actualObj.getFields();
	                while(itr.hasNext()) {
	                	Entry<String, JsonNode> next = itr.next();
	                	graphiteWriter.printf("%s.%s %f %d\n", p.name, next.getKey(), next.getValue().asDouble(), graphiteTimestamp);
	                }
	                continue; 
	            } catch (JsonParseException e) {
	            	LOG.trace("metric was not given in nither in valid JSON format, will try double");
	            } catch (IOException e) {
	            	LOG.trace("metric was not given in nither in valid JSON format, will try double");
	            }
	            try {
	            	graphiteWriter.printf("%s %f %d\n", p.name, Double.parseDouble((String) p.value), graphiteTimestamp);
	            } catch(NumberFormatException e) {
	            	LOG.trace("metric was not given in nither in valid JSON format nor in double format");
	            }*/
	            
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