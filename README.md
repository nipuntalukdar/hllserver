*hllserver*
=========
A high-performance hyperloglog server
------------------------------------

Hllserver implements hyperloglog algortithm and makes the service available over the
network. It is useful for getting nearly distinct-count for high-cardinality multisets. Calculating
distinct element count for high-cardinality multisets may consume a lot of memory, and this is the
problem solved by hyperloglog probabilistic algorithm which returns an approximate value of
cardinality by using minimal memory. 

**Hllserver** supports both HTTP and Thrift protocol and hence can be easily used from clients written in any language. 

It supports persistence(optional) and hence logs can be stored in database. Currently it uses boltdb
to store the logs. The logs are written to DB only when an update operation changes its content.
But persistsing the data to store is not synchronous and some updates may be lost. The changes are
committed to store every second, which means we may end up losing updates for last 1 second in case of non-graceful shutdown. Delete of log keys are synchronus.

It doesn't support replication and hence it is not an highly available service. I may add that (using RAFT consensus protocol most probably) if the need arises. 

Installation
------------
You need to have golang >= 1.5 installed on your machine as I haven't uploaded the prebuilt binary
anywhere. Use the below command to install hyperloglog server:

**$ go install github.com/nipuntalukdar/hllserver/hllserverd**

Running hllserver processs
--------------------------

Running the server:

**$hllserverd -db /tmp -persist**

Detailed usage is shown below:
```bash
$ hllserverd --help
Usage of hllserverd:
  -db string
        directory for hyperlog db (default "/tmp")
  -dbfile string
        hyperlogdb file (default "hyperlogs.db")
  -http string
        give http lister_address (default ":55123")
  -logbackup int
        maximum backup for logs (default 10)
  -logfile string
        log file path (default "/tmp/hllogs.log")
  -logfilesize int
        log rollover size (default 2048000)
  -loglevel string
        logging level (default "INFO")
  -persist
        should persist the hyperlogs in db?
  -thrift string
        thrift rpc address (default "127.0.0.1:55124")
```

HTTP API examples with curl
------------------------------
Below are the HTTP APIs:

1. Adding a new log  
**/addlogkey?logkey=<key>&expiry=<expiry-value-in-seconds>**
```bash
    parameter expiry is optional and by default expiry value is 0 which means the logkey will never expire. The expiry value denotes the number of seconds from current time when the logkey will expire. Examples are given below:
    $ curl "http://127.0.0.1:55123/addlogkey?logkey=key1"
    $ curl "http://127.0.0.1:55123/addlogkey?logkey=key2&expiry=1234"
    On success, the server sends 200 OK status with the JSON body 
    {"status":"success"}
```
2. Deleting an existing log entry  
**/dellogkey?logkey=<key>**
```bash
    parameter logkey specifies the key to be deleted. An example is shown below:
    $ curl "http://127.0.0.1:55123/dellogkey?logkey=key2"
    It generally always returns 200 OK response with the below JSON body (even for non-existing logkeys)
    {"status":"success"}
```
3. Updating log   
**/updatelog**  
 This is the API to "add" entries to the multiset. We need to post a JSON document with two keys: logkey and values. **logkey** points to the log to be updated and **values** point to an array of base64 encoded values to be "added" to the the multiset. 
```bash
An example post document is shown below:
{"logkey" : "key100",  "values": ["77u/VGhl\n", "UHJvamVjdA==\n", "R3V0ZW5iZXJn\n", "RGlja2Vucw==\n", "UG9zdGluZw==\n"]}

Example commands to update multiset pointed by the log keys:

$ curl -XPOST http://127.0.0.1:55123/updatelog -d '{"logkey" : "key1",  "values": [ "c2lzdGVyTXJz\n", "Sm9l\n", "R2FyZ2VyeQ==\n", "d2hv\n", "bWFycmllZA==\n", "dGhl\n" ]} '

$ curl -XPOST http://127.0.0.1:55123/updatelog -d ' {"logkey" : "key1",  "values": ["77u/VGhl\n", "UHJvamVjdA==\n", "R3V0ZW5iZXJn\n", "RUJvb2s=\n", "b2Y=\n", "R3JlYXQ=\n", "RXhwZWN0YXRpb25z\n", "Ynk=\n", "Q2hhcmxlcw==\n"]} '

$ curl  http://127.0.0.1:55123/cardinality?logkey=key1
       Resonse: {"cardinality":14,"status":"success"}
```
4. Get cardinality  
**/cardinality?logkey=<key>**
```bash
This API returns the cardinality of the multiset associated with a given log key. Parameter **logkey** holds the log key identifier.

Example:
$ curl  http://127.0.0.1:55123/cardinality?logkey=key1
       Resonse: {"cardinality":14,"status":"success"}
```
5. Update expiry of a log key  
**/updexpiry?logkey=<key>&expiry=<expiry-in-seconds-from-current-time>**
```bash
This API updates the expiry time of a log key. Parameter **logkey** holds the log key identifier. Parameter **expiry** holds the expiry time in seconds from current time when the log key should expire.

Example:
$curl  "http://127.0.0.1:55123/updexpiry?logkey=key1&expiry=10000"
```

TODO
-----
Hyperloglog++ algorithm has some enhancements over the original hyperloglog algorith. I am planning to add support for hyperloglog++ algorithm as well very soon.

