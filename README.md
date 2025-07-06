# ReactParquet

A simple java lib to read parquet files using Spring Reactor approach.

The main benefit is to use a parquet file as an event source to subscribe to, when your records will only be read on demand.

This actual implementation convert parquet rows in Map<String, Object>, making it simple to consume without requiring conversion into an object. However, it could be evolved in the future to support set-top boxes.


**Contributions and tips are welcome.**

