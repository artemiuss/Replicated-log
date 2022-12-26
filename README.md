# Replicated Log Task

Team 6:
- Dmytro Kushnir
- Artem Avetian
- Nikita Korsun

## Simple Echo-Server application
`echo-server.py` -- implementation of simple Echo-Server application. Accept requests on the 8080 port. Supports only GET and POST requests.

### Usage notes
1. Start Echo-Server application
```
chmod echo-server.py
./echo-server.py
```

2. Send GET requests:
```
curl localhost:8080
curl "localhost:8080?key1=value1"
curl "localhost:8080?key1=value1&key2=value2"
curl "localhost:8080?key1=value1&key2=value2&key3=value3"
```

3. Send POST requests with JSON data:
```
curl -X POST localhost:8080 -H 'Content-Type: application/json' -d '{"key1":"value1"}'
curl -X POST localhost:8080 -H 'Content-Type: application/json' -d '{"key1":"value1", "key2":"value2"}'
curl -X POST localhost:8080 -H 'Content-Type: application/json' -d '{"key1":"value1", "key2":"value2", "key3":"value3"}'
```

## Replicated log
- 

Usage notes:
```
docker-compose up
```
master port
secondery port
json