package com.github.aloomaio.storm.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * This class serves as a reporter thread that handles connection to graphite.
 * This is more efficient compared to v. 0.1, as now the connection with graphite can be persistent.
 * This class does not provide any input validation whatsoever
 */
public class GraphiteMetricsReporter extends Thread {
	public static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricsReporter.class);
	
	/***
	 * Events waiting to be reported to graphite
	 */
	private ConcurrentLinkedQueue<String> waitingToReport;
	
	private String graphiteHost;
	private int graphitePort;
	
	/***
	 * Can signal thread stop
	 */
	private boolean shouldRun = true;
	
	public GraphiteMetricsReporter(String graphiteHost, int graphitePort) {
		this.graphiteHost = graphiteHost;
		this.graphitePort = graphitePort;
		this.waitingToReport = new ConcurrentLinkedQueue<String>();
	}
	
	/***
	 * Enqueue a string to be written to graphite
	 */
	public boolean enqueue(String s) {
		return waitingToReport.add(s);
	}
	
	/***
	 * Signal this thread it should go down.
	 */
	public void signalStop() {
		this.shouldRun = false;
	}

	@Override
	public void run() {
		Socket socket = null;
		PrintWriter graphiteWriter = null;
		while (shouldRun) {
			try {
				/*
				 * Connect to graphite and hold the connection open as long as possible
				 */
				LOG.info(String.format("Connecting to graphite on %s:%d", graphiteHost, graphitePort));
				socket = new Socket(graphiteHost, graphitePort);
				graphiteWriter = new PrintWriter(socket.getOutputStream(), true);
				while(shouldRun) { /* as long the socket is active and the thread was not signaled to stop*/
					String next = waitingToReport.poll();
					if(null == next) {
						/* nothing in the queue */
						Thread.sleep(100);
					} else {
						LOG.trace("Reporting {} to graphite", next);
						graphiteWriter.printf("%s\n", next); /* this can fail if the socket disconnected, but we can live with losing a data point*/
					}
				}
			} catch(IOException e) {
				/* The socket probably disconnected */
				LOG.warn("IOException with graphite socket, will reconnect", e);
			} catch(InterruptedException e) {
				LOG.warn("Interrupted during sleep on graphite reporter thread", e);
			} catch(Throwable t) {
				LOG.error("Caught throwable in graphite reporter thread", t);
			} finally {
				if(null != socket) {
					try {
						socket.close();
					} catch (IOException e) {
						LOG.error("Exception when closing socket", e);
					}
				}
			}
		}
		LOG.info("graphite reporter thread was signalled to stop, shouldRun value is {}", shouldRun);
		if(null != graphiteWriter) {
			graphiteWriter.close();
		}
		if(null != socket) {
			try {
				socket.close();
			} catch (IOException e) {
				LOG.error("Exception when closing socket", e);
			}
		}
	}
}
