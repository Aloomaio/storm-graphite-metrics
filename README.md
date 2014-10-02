#storm-graphite-metrics
Metrics consumer for apache storm that exports metrics from Storm into graphite, using Storm's Metric Consumer interface.

##Usage
In Storm configuration
```java
   conf.registerMetricsConsumer(GraphiteMetricsConsumer.class, 1);
   conf.put(GraphiteMetricsConsumer.GRAPHITE_HOST, "<your graphite host>"); //default localhost
   conf.put(GraphiteMetricsConsumer.GRAPHITE_PORT, "<your graphite port>"); //default 2003
```
##Configure Output Format
Graphite expects incoming data in the format of <path.to.metric> <numeric value> <timestamp>
Some of the transmitted metrics will have inner key-value pairs (e.g. HashMap), while others come as raw value.

For metrics which are coming in with key value pair the default format of the
path to metric is `host.port.componentId.taskId.metricName.metricKey`

For metrics which are coming in as raw values the default format is `host.port.componentId.taskId.metricName`

These formats can be overriden via Storm configuration
```java
    conf.put(GraphiteMetricsConsumer.GRAPHITE_OUTPUT_FORMAT, "%swh.%swp.%sci.%sti.%mn.%mk"); //this is the default
    conf.put(GraphiteMetricsConsumer.GRAPHITE_OUTPUT_FORMAT_SINGLE, "%swh.%swp.%sci.%sti.%mn"); //this is the default
```
The enum OutputFormatConstructs can also be used to generate these Strings, e.g.
```java
    conf.put(GraphiteMetricsConsumer.GRAPHITE_OUTPUT_FORMAT, String.format("%s.%s.%s.%s.%s.%s",
			OutputFormatConstructs.SRC_WORKER_HOST.magicSequence,
			OutputFormatConstructs.SRC_WORKER_PORT.magicSequence,
			OutputFormatConstructs.SRC_COMPONENT_ID.magicSequence,
			OutputFormatConstructs.SRC_TASK_ID.magicSequence,
			OutputFormatConstructs.METRIC_NAME.magicSequence,
			OutputFormatConstructs.METRIC_KEY.magicSequence));
```

##Download
The jar is distributed via maven central.
```xml
<dependency>
    <groupId>com.github.aloomaio</groupId>
    <artifactId>storm-graphite-metrics</artifactId>
    <version>0.1</version>
</dependency>
```
