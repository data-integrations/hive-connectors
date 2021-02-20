# Hive Batch Sink


Description
-----------
Converts a StructuredRecord to a HCatRecord and then writes it to an existing Hive table.


Configuration
-------------
**referenceName:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**metastoreURL:** The URL of Hive metastore in the format ``thrift://<hostname>:<port>``.
Example: ``thrift://somehost.net:9083``. (Macro-enabled)


**database:** The name of the database. Defaults to 'default'. (Macro-enabled)

**table:** The name of the Hive table. This table must exist. (Macro-enabled)

**partition filter:** Optional Hive expression filter for writing, provided as a JSON Map of key-value pairs that
describe all the partition keys and values for that partition. For example: if the partition column is 'type',
then this property should be specified as ``{"type": "typeOne"}``.
To write multiple partitions simultaneously you can leave this empty; but all the partitioning columns must
be present in the data you are writing to the sink. (Macro-enabled)

**properties:** Any extra properties to include. The property-value pairs should be comma-separated,
and each property should be separated by a colon from its corresponding value. (Macro-enabled)

Example
-------
This example connects to a Hive database 'db' using the specified URL 'metastoreURL'. It performs a select query on the
selected table 'tb' based on the given partition filter 'partitionFilter' (if provided).  'connectionProperties' are
extra properties that can be used to modify connection properties.

```json
{
    "name": "Hive",
    "type": "batchsource",
    "properties": {
        "metastoreURL": "thrift://somehost.net:9083",
        "database": "db",
        "table": "tb",
        "partitionFilter": "column = value",
        "connectionProperties": "key1=value1;key2=value2"
    }
}
```
