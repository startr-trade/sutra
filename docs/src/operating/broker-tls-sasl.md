# Connecting to a TLS and SASL broker

Since 0.6.21 the `kafka` and `amqp` (AMQP 1.0) transports connect to TLS brokers natively, with no sidecar. Kafka
also authenticates with SASL PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, and AMQP 1.0 with SASL PLAIN.

**What this was tested against.** Local containers only: Apache Kafka 3.8 and Apache ActiveMQ
Artemis. Azure Service Bus, Confluent Cloud, Amazon MSK and other managed services have not been
tested and are not claimed to work. Azure Service Bus's CBS token flow is not implemented. Client
certificates (mTLS) and SASL OAUTHBEARER and GSSAPI are not supported.

## The settings

For an inbound channel:

| Property | Transport | Meaning |
|---|---|---|
| `security.protocol` | kafka | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `sasl.mechanism` | kafka | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `sasl.username`, `sasl.password` | kafka | Secret references |
| `tls: true` | amqp | Use TLS. The default port becomes 5671 |
| `username`, `password` | amqp | Secret references. SASL PLAIN is used when both are set |
| `tls-ca-file` | both | Path to a PEM bundle, used in place of the system CAs |
| `tls-ca-pem` | both | Secret reference to PEM text, used in place of the system CAs |
| `tls-insecure-skip-verify` | both | `true` turns off certificate and host-name checks |

A credential, and `tls-ca-pem`, must be a reference as a whole value: `env:NAME`, `secret:KEY`, `${NAME}`,
`${secret:KEY}`, a vendor store scheme the engine is built with, or `k8s:secret/<name>#<key>` (see
[References in connection properties](connection-references.md)). A literal value stops the channel at
startup with `SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`. `tls-ca-file` takes references like an address does;
the switches (`tls`, `security.protocol`, `sasl.mechanism`, `tls-insecure-skip-verify`) stay literal.

With no CA setting, the broker is checked against the system CAs, including any set with
`SSL_CERT_FILE`. Certificate and host-name checks are on unless you set `tls-insecure-skip-verify:
true`, and the engine logs a warning whenever that setting applies.

A setting that could not take effect stops the channel at startup rather than being ignored: a CA
without TLS, `sasl.*` without a SASL protocol, both CA settings at once, or SASL GSSAPI or OAUTHBEARER. Kafka's `kafka.*`
passthrough does not accept `sasl.*`, `ssl.*` or `enable.ssl.certificate.verification`.

Outbound sends use one engine-wide configuration per transport, set in the environment:

| Variable | Meaning |
|---|---|
| `SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS` | The brokers |
| `SUTRA_SINK_KAFKA_SECURITY_PROTOCOL` | As `security.protocol` |
| `SUTRA_SINK_KAFKA_SASL_MECHANISM`, `_SASL_USERNAME`, `_SASL_PASSWORD` | As the `sasl.*` properties. These hold values, so set them from your secret store |
| `SUTRA_SINK_KAFKA_TLS_CA_FILE`, `SUTRA_SINK_KAFKA_TLS_INSECURE_SKIP_VERIFY` | Trust |
| `SUTRA_SINK_AMQP_USERNAME`, `SUTRA_SINK_AMQP_PASSWORD` | Default credentials |
| `SUTRA_SINK_AMQP_TLS_CA_FILE`, `SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY` | Trust for `amqp10s://` destinations |

## Walkthrough: a message through both brokers

You need Docker, `openssl`, the `sutra` CLI and an empty working directory. This creates a private
CA, starts both brokers with TLS and a login, and runs a package that answers each message. It was run
as written, with sutra 0.6.21, `apache/kafka:3.8.0`, `apache/activemq-artemis:latest-alpine` and
`postgres:16-alpine`; the output shown is from that run.

**1. Create the certificates.** The server certificate must name the host the engine connects to.

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 7 \
  -subj "/CN=demo broker CA" -keyout ca.key -out ca.pem
for broker in tlsdemo-kafka tlsdemo-artemis; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$broker" -keyout "$broker.key" -out "$broker.csr"
  openssl x509 -req -in "$broker.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 7 \
    -extfile <(printf "subjectAltName=DNS:%s" "$broker") -out "$broker.crt"
done
chmod 644 ./*.key ./*.crt ca.pem
BROKER_PASSWORD=$(openssl rand -hex 16)
docker network create tlsdemo-net
```

The `chmod` lets the containers read these files. The keys are throwaway, so don't reuse this step
for real keys.

**2. Start Kafka** with a plaintext listener inside its container for the commands below, and a
SASL_SSL listener for the engine. Save this as `server.properties`:

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
listeners=INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SECURE://0.0.0.0:9094
advertised.listeners=INTERNAL://localhost:9092,SECURE://tlsdemo-kafka:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SECURE:SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.secure.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PEM
ssl.keystore.location=/certs/kafka-keystore.pem
log.dirs=/tmp/kraft-logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

```bash
cat tlsdemo-kafka.key tlsdemo-kafka.crt > kafka-keystore.pem
chmod 644 kafka-keystore.pem server.properties
docker run -d --name tlsdemo-kafka --network tlsdemo-net \
  -v "$PWD/server.properties:/certs/server.properties:ro" \
  -v "$PWD/kafka-keystore.pem:/certs/kafka-keystore.pem:ro" \
  --entrypoint /bin/sh apache/kafka:3.8.0 -c "
    /opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /certs/server.properties \
      --add-scram 'SCRAM-SHA-256=[name=orders-svc,password=$BROKER_PASSWORD]' &&
    exec /opt/kafka/bin/kafka-server-start.sh /certs/server.properties"
until docker logs tlsdemo-kafka 2>&1 | grep -q 'Kafka Server started'; do sleep 1; done
for topic in orders-in orders-out; do
  docker exec tlsdemo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$topic"
done
```

**3. Start Artemis** with an AMQPS acceptor.

```bash
openssl pkcs12 -export -in tlsdemo-artemis.crt -inkey tlsdemo-artemis.key -out artemis.p12 \
  -passout pass:changeit -name broker
chmod 644 artemis.p12
docker run -d --name tlsdemo-artemis --network tlsdemo-net \
  -v "$PWD/artemis.p12:/certs/artemis.p12:ro" \
  --entrypoint /bin/sh apache/activemq-artemis:latest-alpine -c "
    /opt/activemq-artemis/bin/artemis create --user orders-svc --password $BROKER_PASSWORD \
      --silent --require-login --http-host 0.0.0.0 --relax-jolokia . &&
    sed -i 's#</acceptors>#<acceptor name=\"amqps\">tcp://0.0.0.0:5671?sslEnabled=true;keyStorePath=/certs/artemis.p12;keyStorePassword=changeit;keyStoreType=PKCS12;protocols=AMQP;saslMechanisms=PLAIN</acceptor></acceptors>#' etc/broker.xml &&
    exec ./bin/artemis run"
until docker logs tlsdemo-artemis 2>&1 | grep -q 'Server is now active'; do sleep 1; done
for q in orders-in orders-out; do
  docker exec tlsdemo-artemis ./bin/artemis queue create --user orders-svc --password "$BROKER_PASSWORD" \
    --url tcp://localhost:61616 --name "$q" --address "$q" --anycast --durable --auto-create-address \
    --preserve-on-no-consumers --silent
done
```

The two queues are created up front, as anycast queues, so a message sent before anyone reads it is
kept. Left to Artemis, an address the engine attaches to first is created as multicast, and the
producer below would send to a queue no one reads.

**4. Write the package.** Create `tls-relay/package.yaml`:

```yaml
labels:
  "tenant": "default"
  "module": "tls-relay"
  "version": "1.0.0"
engine:
  minContract: 1
```

Create `tls-relay/channels.yaml`:

```yaml
channels:
  - name: kafka-in
    transport: kafka
    codec: json
    cloudevents-mode: none
    bootstrap.servers: tlsdemo-kafka:9094
    topic: orders-in
    group.id: tls-relay
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-256
    sasl.username: env:BROKER_USER
    sasl.password: secret:BROKER_PASSWORD
    tls-ca-pem: secret:BROKER_CA
  - name: kafka-out
    direction: outbound
    transport: kafka
    bind: kafka://orders-out
    cloudevents-mode: none
  - name: amqp-in
    transport: amqp
    codec: json
    cloudevents-mode: none
    host: tlsdemo-artemis
    tls: true
    tls-ca-pem: secret:BROKER_CA
    username: env:BROKER_USER
    password: secret:BROKER_PASSWORD
    queue: orders-in
  - name: amqp-out
    direction: outbound
    transport: amqp
    bind: amqp10s://tlsdemo-artemis/orders-out
    cloudevents-mode: none
```

Create `tls-relay/bpmn/tls-relay.bpmn`, with one process per transport. Each builds an answer and
sends it out. This is the Kafka one; add a copy for AMQP with its ids prefixed `amqp`, its process id
`amqp-relay`, and the channels `amqp-in` and `amqp-out`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="Defs"
                  targetNamespace="urn:sutra:module:tls-relay:1.0.0">
  <bpmn:process id="kafka-relay" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables><q:variable name="responseBody" type="object"/></q:variables>
    </bpmn:extensionElements>
    <bpmn:startEvent id="kafkaStart">
      <bpmn:extensionElements><q:source channel="kafka-in"/></bpmn:extensionElements>
      <bpmn:outgoing>kafkaF1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="kafkaF1" sourceRef="kafkaStart" targetRef="kafkaBuild"/>
    <bpmn:serviceTask id="kafkaBuild">
      <bpmn:incoming>kafkaF1</bpmn:incoming><bpmn:outgoing>kafkaF2</bpmn:outgoing>
      <bpmn:dataInputAssociation>
        <bpmn:assignment>
          <bpmn:from>{ref: payload.ref, status: "ANSWERED"}</bpmn:from>
          <bpmn:to>responseBody</bpmn:to>
        </bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="kafkaF2" sourceRef="kafkaBuild" targetRef="kafkaAnswer"/>
    <bpmn:sendTask id="kafkaAnswer">
      <bpmn:extensionElements><q:send channel="kafka-out"/></bpmn:extensionElements>
      <bpmn:incoming>kafkaF2</bpmn:incoming><bpmn:outgoing>kafkaF3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="kafkaF3" sourceRef="kafkaAnswer" targetRef="kafkaEnd"/>
    <bpmn:endEvent id="kafkaEnd"><bpmn:incoming>kafkaF3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
```

```bash
mkdir -p deployments secrets
sutra lint tls-relay
sutra package tls-relay --out deployments
```

```text
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/tls-relay/1.0.0 (...): channel 'kafka-in' has 1 subscriber(s) but its codec (json) declares no message types — ...
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/tls-relay/1.0.0 (...): channel 'amqp-in' has 1 subscriber(s) but its codec (json) declares no message types — ...
0 error(s), 2 warning(s)
packaged deployments/tls-relay.sutra (deploymentId dep-e890994c8e67a1c5b02e6db2)
```

The two warnings say the channels are schema-less, which is what this package intends.

**5. Start the engine.** The inbound channels read the password and the CA from the mounted secrets
directory. The engine-wide sinks take theirs from the environment. The database role has no
superuser or `BYPASSRLS` rights, because the engine refuses to start on one that does.

```bash
printf '%s' "$BROKER_PASSWORD" > secrets/BROKER_PASSWORD
cp ca.pem secrets/BROKER_CA
chmod -R a+rX deployments secrets
docker run -d --name tlsdemo-db --network tlsdemo-net -e POSTGRES_PASSWORD=dev-only postgres:16-alpine
until docker exec tlsdemo-db pg_isready -U postgres -h 127.0.0.1; do sleep 1; done
sleep 2
docker exec tlsdemo-db psql -U postgres \
  -c "CREATE ROLE sutra_engine LOGIN PASSWORD 'dev-only' NOSUPERUSER NOBYPASSRLS" \
  -c "ALTER SCHEMA public OWNER TO sutra_engine"
docker run -d --name tlsdemo-engine --network tlsdemo-net -p 127.0.0.1::8080 \
  -v "$PWD/deployments:/deployments:ro" -e SUTRA_DEPLOYMENTS_DIR=/deployments \
  -v "$PWD/secrets:/etc/sutra/secrets:ro" \
  -v "$PWD/ca.pem:/etc/sutra/broker-ca.pem:ro" \
  -e SUTRA_DATASOURCE_URL=postgres://tlsdemo-db:5432/postgres \
  -e SUTRA_DATASOURCE_USERNAME=sutra_engine -e SUTRA_DATASOURCE_PASSWORD=dev-only \
  -e BROKER_USER=orders-svc \
  -e SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS=tlsdemo-kafka:9094 \
  -e SUTRA_SINK_KAFKA_SECURITY_PROTOCOL=SASL_SSL \
  -e SUTRA_SINK_KAFKA_SASL_MECHANISM=SCRAM-SHA-256 \
  -e SUTRA_SINK_KAFKA_SASL_USERNAME=orders-svc \
  -e SUTRA_SINK_KAFKA_SASL_PASSWORD="$BROKER_PASSWORD" \
  -e SUTRA_SINK_KAFKA_TLS_CA_FILE=/etc/sutra/broker-ca.pem \
  -e SUTRA_SINK_AMQP_USERNAME=orders-svc \
  -e SUTRA_SINK_AMQP_PASSWORD="$BROKER_PASSWORD" \
  -e SUTRA_SINK_AMQP_TLS_CA_FILE=/etc/sutra/broker-ca.pem \
  ghcr.io/startr-trade/sutra:0.6.21
PORT=$(docker port tlsdemo-engine 8080 | head -1 | cut -d: -f2)
until curl -sf "http://127.0.0.1:$PORT/sutra/health/ready"; do sleep 1; done
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

**6. Send a message through Kafka and read the answer.**

```bash
echo '{"ref":"kafka-1"}' | docker exec -i tlsdemo-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders-in
docker exec tlsdemo-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-out --from-beginning --max-messages 1 --timeout-ms 60000
```

```text
{"ref":"kafka-1","status":"ANSWERED"}
Processed a total of 1 messages
```

The message went in and the answer came out over the SASL_SSL listener, with SCRAM-SHA-256 and the
private CA; the commands above use the broker's internal plaintext listener only to drive it.

**7. Send a message through Artemis and read the answer.**

```bash
docker exec tlsdemo-artemis ./bin/artemis producer --user orders-svc --password "$BROKER_PASSWORD" \
  --url tcp://localhost:61616 --protocol amqp --destination queue://orders-in --message-count 1 \
  --message '{"ref":"amqp-1"}'
docker exec tlsdemo-artemis ./bin/artemis browser --user orders-svc --password "$BROKER_PASSWORD" \
  --url tcp://localhost:61616 --destination queue://orders-out --message-count 1 --verbose
```

```text
Producer orders-in, thread=0 Produced: 1 messages
...
Consumer ActiveMQQueue[orders-out], thread=0 browsed: 1 messages
```

The engine read the message over AMQPS (port 5671, SASL PLAIN, the private CA) and sent its answer to
`amqp10s://tlsdemo-artemis/orders-out`. The engine's log holds the broker password 0 times.

**8. See a refusal.** Restart the engine with a wrong password in `secrets/BROKER_PASSWORD` (and in the
two `SUTRA_SINK_*_PASSWORD` variables), and read its log:

```bash
docker logs tlsdemo-engine 2>&1 | grep -E 'BROKER_(AUTH|TLS)_FAILED'
```

```text
{"channel":"amqp-in","code":"SUTRA.INBOUND.AMQP.BROKER_AUTH_FAILED",...,"level":"WARN",...,"message":"amqp consumer unavailable — retrying in 1s: amqp channel 'amqp-in' could not connect to tlsdemo-artemis:5671: the broker refused the SASL credentials (outcome Auth)",...}
{"client":"channel 'kafka-in'","code":"SUTRA.INBOUND.KAFKA.BROKER_AUTH_FAILED",...,"level":"WARN",...,"message":"kafka channel 'kafka-in': sasl_ssl://tlsdemo-kafka:9094/bootstrap: SASL authentication error: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256 ...",...}
```

Put the right password back and a certificate from another CA in `secrets/BROKER_CA`, restart, and the
log reads:

```text
{"client":"channel 'kafka-in'","code":"SUTRA.INBOUND.KAFKA.BROKER_TLS_FAILED",...,"message":"kafka channel 'kafka-in': sasl_ssl://tlsdemo-kafka:9094/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified ...",...}
{"channel":"amqp-in","code":"SUTRA.INBOUND.AMQP.BROKER_TLS_FAILED",...,"message":"amqp consumer unavailable — retrying in 1s: amqp channel 'amqp-in' could not connect to tlsdemo-artemis:5671: the TLS handshake failed: invalid peer certificate: UnknownIssuer",...}
```

Neither the wrong password nor the right one appeared anywhere in the log.

| Code | Meaning |
|---|---|
| `SUTRA.{INBOUND,OUTBOUND}.{KAFKA,AMQP}.BROKER_AUTH_FAILED` | The broker refused the credentials |
| `SUTRA.{INBOUND,OUTBOUND}.{KAFKA,AMQP}.BROKER_TLS_FAILED` | The CA does not trust the broker's certificate, the certificate does not name the host, or the trust settings could not be loaded |

Neither message includes a password. Both consumers keep retrying and an outbound send stays in the
outbox, so both recover once you correct the setting and restart the engine. The AMQP consumer backs
off (1 s, 2 s, ...). The Kafka consumer logs one line per attempt by its client, which retries quickly:
with the wrong CA this run logged about 740 `BROKER_TLS_FAILED` lines in 20 seconds, so alert on the code
rather than on the line count.

**9. Clean up.**

```bash
docker rm -f tlsdemo-engine tlsdemo-db tlsdemo-artemis tlsdemo-kafka
docker network rm tlsdemo-net
```
