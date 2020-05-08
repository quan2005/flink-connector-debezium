# Flink-connector-debezium

基于Red Hat开源的Debezium组件，和Flink结合，实现在Flink上直接监听各种数据库的Change Log，并且能够利用Flink的特性，输出到不同的位置上，以实现数据总线和实时数仓的构建。此项目主要应用于数据中台或数据平台中数据总线的基础底座。

目前Debezium支持了MySQL、MongoDB、PostgreSQL、Oracle、SQL Server、Db2和Cassandra。理论上此连接器也支援以上所有数据库。

## Getting Started

Start by not writing any code.

```
mvn clean package -DskipTests
```

example

参考DebeziumSourceITCase
```
docker run -d -p 3306:3306 --name mysql mysql:5 
```
