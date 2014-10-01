#storm-graphite-metrics
Metrics consumer for apache storm that exports metrics into graphite

##Usage
In Storm configuration
```java
   conf.registerMetricsConsumer(GraphiteMetricsConsumer.class, 1);
   conf.put(GraphiteMetricsConsumer.GRAPHITE_HOST_KEY, "<your graphite host>"); //default localhost
   conf.put(GraphiteMetricsConsumer.GRAPHITE_PORT_KEY, "<your graphite port>"); //default 2003
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
