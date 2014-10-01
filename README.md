storm-graphite-metrics
======================

Metrics consumer for apache storm that exports metrics into graphite

usage
=====

In Storm configuration
```java
   conf.registerMetricsConsumer(GraphiteMetricsConsumer.class, 1);
   //default localhost
   conf.put(GraphiteMetricsConsumer.GRAPHITE_HOST_KEY, "<your graphite host>");
   //default 2003
   conf.put(GraphiteMetricsConsumer.GRAPHITE_PORT_KEY), "<your graphite port>")
```

