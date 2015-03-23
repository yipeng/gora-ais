# gora-ais

There are AIS decoders of varying quality on the web but almost none have database integration. This project aims to provide a general interface to connect ANY ais decoder to ALL of the storage engines supported by Apache Gora, including Column stores such as Apache HBase™, Apache Cassandra™, Hypertable; key-value stores such as Voldermort, Redis, etc; SQL databases, such as MySQL, HSQLDB, flat files in local file system of Hadoop HDFS.

At present, it stores raw NMEA AIS from files with an Avro schema and HBase mapping. 
