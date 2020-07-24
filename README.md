# Feast Load Test Proxy

This simple Go service generates load as part of the Feast testing suite. It sits between an HTTP based load testing tool as follows:

```
[Load Testing Tool] --(http)--> [Feast Load Test Proxy] --(grpc)--> [Feast Serving]
```

### Usage
Start the proxy
```
LOAD_FEAST_SERVING_HOST=feast.serving.example.com LOAD_FEAST_SERVING_PORT=6566 go run main.go
```
```
2020/07/24 14:00:02 Creating client to connect to Feast Serving at localhost:6566
2020/07/24 14:00:02 Starting server on port:8080
```

The following command simply echos the version of Feast Serving. Useful for testing network latency
```
curl localhost:8080/echo
```

This command will send a single GetOnlineFeatures request to the configured Feast serving instance. The `entity_count` parameter is used to set how many entities will be sent (unique users in this case). The higher the number of entities the higher the expected latency.

```
curl localhost:8080/send?entity_count=30
```