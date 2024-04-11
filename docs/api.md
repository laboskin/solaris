## CURLing the API
Below are examples of how to use CURL to call the **Solaris** API.

##### GET /logs
Retrieve existing logs
```
curl -v -s -G -XGET "http://localhost:8080/v1/logs?limit=10" | jq
```

##### GET /logs (with filters)
Retrieve existing logs with filters
```
curl -v -s -G -XGET --data-urlencode "logsCondFilter=tag('a')='b' and tag('c')='d'" "http://localhost:8080/v1/logs?limit=10" | jq
```

##### POST /logs
Create a new log
```
curl -v -s -XPOST -H "content-type: application/json" -d '{"tags":{"a":"b", "c":"d"}}' "http://localhost:8080/v1/logs" | jq
```

##### DELETE /logs
Delete logs by filter condition
```
curl -v -s -XDELETE -H "content-type: application/json" -d '{"filterCondition":"logID = \"01HV6Y4YTNPC64VDYBDF1VW3KM\" or tag(\"a\")=\"b\""}' "http://localhost:8080/v1/logs" | jq
```

##### PUT /logs/{id}
Update log tags
```
curl -v -s -XPUT -H "content-type: application/json" -d '{"tags":{"a":"b", "c":"e"}, "records":"100"}' "http://localhost:8080/v1/logs/01HV523WYP0ZSDAYEJ4JNED6F7" | jq
```

##### POST /logs/{id}/records
Add records to the log
```
curl -v -s -XPOST -H "content-type: application/json" -d "{\"records\":[{\"payload\":\"`echo 'data123' | base64`\"}, {\"payload\":\"`echo 'data456' | base64`\"}]}" http://localhost:8080/v1/logs/01HV523WYP0ZSDAYEJ4JNED6F7/records | jq
```

##### GET /records
Retrieve existing records
```
curl -v -s -G -XGET "http://localhost:8080/v1/records?limit=10" | jq
```

##### GET /records (with filters)
Retrieve existing records with filters
```
curl -v -s -G -XGET --data-urlencode "logsCondFilter=tag('a')='b' and tag('c')='d' or logID = '01HV6YH47B2MQBAPRTYV9KB7ZK'" --data-urlencode "recordsCondFilter=ctime > '2024-04-11T16:06:40.63Z' and ctime < '2024-04-11T16:06:51.59Z'" "http://localhost:8080/v1/records?limit=10" | jq
```
