# Langolier
Langolier is a CLI tool to consume a Kafka topic and emit tombstones for messages matched by header name/value pairs.

## Usage
```
Usage of langolier:
  -header string
    	header name to match on (default "tenantId")
  -log-bad
    	log bad messages which have no headers
  -log-neg
    	log negative matched messages
  -log-pos
    	log positive matched messages (default true)
  -log-ts
    	log tombstone messages consumed (default true)
  -partitions string
    	partitions to process (default 'all'; comma-delimited) (default "all")
  -start string
    	start time (inclusive; default earliest) (default "0")
  -stop string
    	stop time (inclusive; default now) (default "now")
  -topic string
    	kafka topic name; REQUIRED
  -values string
    	header values to match on (where value in [values]; comma-delimited; REQUIRED)
```

To enable a flag, use e.g. `-log-bad=true`.
To disable a flag, use e.g. `-log-pos=false`.

The `-start` and `-stop` arguments can be absolute times in RFC3339 format, or now-relative time expressions
e.g. `now-4h`, `now-1d`, `now+1d-3w4mo+7y6h4m`.

The `-header` argument is the name of the header key to match on.

The `-values` argument is a comma-delimited list of header values to match on for the header name.
A successful match for the message occurs when _any_ of the header values matches the message's header value.

## Environment Variables

| Variable | Default | Description |
| --------- | -- | -- |
| KAFKA_SERVERS | localhost:9092 | Comma-delimited list of host:port pairs to identify brokers |
| KAFKA_TLS_ENABLE | 0 | Enable TLS connection to Kafka brokers (0 = disabled, 1 = enabled) |
| KAFKA_TLS_ROOT_CA | | Base64 encoded PEM root certificates for TLS connection |
| KAFKA_SASL_ENABLE | 0 | Enable SASL authentication to Kafka brokers (0 = disabled, 1 = enabled) |
| KAFKA_SASL_TYPE | PLAINTEXT | Select between PLAINTEXT or GSSAPI SASL mechanism if SASL enabled |
| KAFKA_SASL_GSSAPI_AUTH | KEYTAB | Select between KEYTAB or USER credentials for GSSAPI mechanism |
| KAFKA_SASL_GSSAPI_KRB5CONF | | Base64-encoded krb5.conf file if GSSAPI enabled |
| KAFKA_SASL_GSSAPI_USERNAME | | Username (without @realm) for GSSAPI |
| KAFKA_SASL_GSSAPI_REALM | | Realm for GSSAPI |
| KAFKA_SASL_GSSAPI_KEYTAB | | Base64-encoded keytab if using KEYTAB credentials |
| KAFKA_SASL_GSSAPI_PASSWORD | | Password if using USER credentials |
| KAFKA_TMP | /tmp/kafka | Temporary directory to store kafka configuration files; a folder with the process ID is created |

If receiving errors about common name validation failing for TLS, `export GODEBUG="x509ignoreCN=0"` and re-run the tool.
