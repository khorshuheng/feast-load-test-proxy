# Feast Load Test Proxy

This simple Go service generates load as part of the Feast testing suite. It sits between an HTTP based load testing tool as follows:

```
[Load Testing Tool] --(http)--> [Feast Load Test Proxy] --(grpc)--> [Feast Serving]
```

### Usage
Create a specification file for the load. Refer to the example specification for details.
```
LOAD_SPECIFICATION_PATH=example/loadSpec.yml
```

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

This command will send a single or multiple GetOnlineFeatures request(s) depending on the load specification.

```
curl localhost:8080/send
```