//! TLS and SASL broker fixtures for the transport suites: test certificates generated at run time
//! (nothing is committed) and an Artemis and a Kafka container configured to require them.
//!
//! A broker fixture can join a docker network under a container name, so an engine container can
//! reach it by a name the certificate covers, while the test process reaches it through mapped
//! ports. Kafka advertises a different address per listener, so it has one listener set for each.

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const ARTEMIS_TLS_PORT: u16 = 5671;
const ARTEMIS_PLAIN_PORT: u16 = 5672;
const KAFKA_HOST_PLAIN_PORT: u16 = 9092;
const KAFKA_HOST_SSL_PORT: u16 = 9094;
const KAFKA_HOST_SASL_SSL_PORT: u16 = 9095;
const KAFKA_HOST_SASL_PLAIN_PORT: u16 = 9096;
/// The in-network SASL_SSL listener's port, for a client on the fixture's docker network.
pub const KAFKA_NETWORK_SASL_SSL_PORT: u16 = 9195;

/// A short value unique to this run, usable as a password or a name: letters and digits only, so
/// it survives the shell and the broker's config formats.
pub fn unique_token(prefix: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!(
        "{prefix}{}x{}x{nanos}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// A throwaway PKI: a CA, a server certificate it signed for the given names, and an unrelated CA
/// for proving refusal. The CA files are world-readable so an unprivileged container can mount them.
pub struct TestBrokerPki {
    /// The issuing CA, PEM.
    pub ca_pem: String,
    /// The issuing CA, as a file.
    pub ca_file: PathBuf,
    /// A CA that did not issue the server certificate, PEM.
    pub foreign_ca_pem: String,
    /// That CA, as a file.
    pub foreign_ca_file: PathBuf,
    /// The server certificate, PEM.
    pub server_cert_pem: String,
    /// The server key, PKCS#8 PEM.
    pub server_key_pem: String,
}

fn new_ca(common_name: &str) -> (String, KeyPair, CertificateParams) {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    let key = KeyPair::generate().expect("CA key");
    let cert = params.self_signed(&key).expect("CA certificate");
    (cert.pem(), key, params)
}

impl TestBrokerPki {
    /// Generate a PKI whose server certificate covers `server_names` (DNS names or IPs).
    pub fn generate(server_names: &[&str]) -> TestBrokerPki {
        let (ca_pem, ca_key, ca_params) = new_ca("sutra test CA");
        let (foreign_ca_pem, _, _) = new_ca("sutra unrelated test CA");
        let issuer = Issuer::new(ca_params, ca_key);

        let names: Vec<String> = server_names.iter().map(|n| n.to_string()).collect();
        let mut params = CertificateParams::new(names).expect("server params");
        params
            .distinguished_name
            .push(DnType::CommonName, server_names[0]);
        let server_key = KeyPair::generate().expect("server key");
        let server_cert = params
            .signed_by(&server_key, &issuer)
            .expect("server certificate");

        let dir = std::env::temp_dir().join(unique_token("sutra-broker-pki-"));
        std::fs::create_dir_all(&dir).expect("create PKI dir");
        make_public(&dir, 0o755);
        let ca_file = dir.join("ca.pem");
        std::fs::write(&ca_file, &ca_pem).expect("write CA");
        make_public(&ca_file, 0o644);
        let foreign_ca_file = dir.join("foreign-ca.pem");
        std::fs::write(&foreign_ca_file, &foreign_ca_pem).expect("write foreign CA");
        make_public(&foreign_ca_file, 0o644);

        TestBrokerPki {
            ca_pem,
            ca_file,
            foreign_ca_pem,
            foreign_ca_file,
            server_cert_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
        }
    }
}

fn make_public(path: &std::path::Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .expect("set permissions");
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
}

/// Where a broker fixture sits on docker, when an engine container must reach it.
#[derive(Debug, Clone, Default)]
pub struct BrokerPlacement {
    /// The docker network to join.
    pub network: Option<String>,
    /// The container name, which is also its address on that network.
    pub container_name: Option<String>,
}

/// A running Artemis that requires a login, with an AMQPS acceptor beside the plain one.
pub struct TlsArtemisBroker {
    /// Held to keep the container alive. Park the fixture in a `static`: dropping it runs a blocking
    /// `docker rm`, which panics on a tokio worker.
    pub container: Container<GenericImage>,
    /// The mapped AMQPS port.
    pub tls_port: u16,
    /// The mapped plaintext AMQP port.
    pub plain_port: u16,
    /// The one broker user.
    pub username: String,
    /// That user's password, generated for this run.
    pub password: String,
}

/// Start Artemis with `pki`'s server certificate on an AMQPS acceptor. The image has `openssl`,
/// which turns the PEM pair into the PKCS#12 store Artemis loads.
pub fn start_tls_artemis(pki: &TestBrokerPki, placement: &BrokerPlacement) -> TlsArtemisBroker {
    let username = "svcbroker".to_string();
    let password = unique_token("pw");
    let store = unique_token("st");
    let script = format!(
        "set -e; \
         openssl pkcs12 -export -in /tmp/tls/server.crt -inkey /tmp/tls/server.key \
           -out /tmp/broker.p12 -passout pass:{store} -name broker; \
         /opt/activemq-artemis/bin/artemis create --user {username} --password {password} \
           --silent --require-login --http-host 0.0.0.0 --relax-jolokia .; \
         sed -i 's#</acceptors>#<acceptor name=\"amqps\">tcp://0.0.0.0:{ARTEMIS_TLS_PORT}?sslEnabled=true;\
keyStorePath=/tmp/broker.p12;keyStorePassword={store};keyStoreType=PKCS12;protocols=AMQP;\
saslMechanisms=PLAIN</acceptor></acceptors>#' etc/broker.xml; \
         exec ./bin/artemis run"
    );
    let mut image = GenericImage::new("apache/activemq-artemis", "latest-alpine")
        .with_exposed_port(ARTEMIS_PLAIN_PORT.tcp())
        .with_exposed_port(ARTEMIS_TLS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server is now active"))
        .with_entrypoint("/bin/sh")
        .with_cmd(["-c".to_string(), script])
        .with_copy_to(
            "/tmp/tls/server.crt",
            pki.server_cert_pem.clone().into_bytes(),
        )
        .with_copy_to(
            "/tmp/tls/server.key",
            pki.server_key_pem.clone().into_bytes(),
        );
    if let Some(network) = &placement.network {
        image = image.with_network(network.clone());
    }
    if let Some(name) = &placement.container_name {
        image = image.with_container_name(name.clone());
    }
    let container = image
        .start()
        .expect("start apache/activemq-artemis:latest-alpine (docker required)");
    crate::reap_on_exit(container.id());
    let tls_port = container
        .get_host_port_ipv4(ARTEMIS_TLS_PORT)
        .expect("mapped AMQPS port");
    let plain_port = container
        .get_host_port_ipv4(ARTEMIS_PLAIN_PORT)
        .expect("mapped AMQP port");
    TlsArtemisBroker {
        container,
        tls_port,
        plain_port,
        username,
        password,
    }
}

/// A running Kafka broker with one listener per security protocol.
pub struct TlsKafkaBroker {
    /// Held to keep the container alive. Park the fixture in a `static`.
    pub container: Container<GenericImage>,
    /// `localhost:<port>` — PLAINTEXT, for a test driver.
    pub plain_bootstrap: String,
    /// `localhost:<port>` — SSL, no client authentication.
    pub ssl_bootstrap: String,
    /// `localhost:<port>` — SASL_SSL.
    pub sasl_ssl_bootstrap: String,
    /// The same SASL_SSL listener's mapped port, for connecting by another name.
    pub sasl_ssl_port: u16,
    /// `localhost:<port>` — SASL_PLAINTEXT.
    pub sasl_plain_bootstrap: String,
    /// The user on every mechanism.
    pub username: String,
    /// That user's password, generated for this run.
    pub password: String,
}

fn free_host_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("allocate a host port")
}

/// Start `apache/kafka` with its own server properties. Host ports are chosen first, because a
/// Kafka listener must advertise the address its clients will reach. SCRAM users exist only once
/// written to the metadata log, which is done at storage format, so the broker starts with them.
pub fn start_tls_kafka(pki: &TestBrokerPki, placement: &BrokerPlacement) -> TlsKafkaBroker {
    let username = "svcbroker".to_string();
    let password = unique_token("pw");
    let plain = free_host_port();
    let ssl = free_host_port();
    let sasl_ssl = free_host_port();
    let sasl_plain = free_host_port();
    let network_name = placement.container_name.as_deref().unwrap_or("localhost");

    let mut properties = format!(
        "process.roles=broker,controller\n\
         node.id=1\n\
         controller.quorum.voters=1@localhost:9093\n\
         controller.listener.names=CONTROLLER\n\
         inter.broker.listener.name=INTER\n\
         listeners=INTER://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,\
HOSTPLAIN://0.0.0.0:{KAFKA_HOST_PLAIN_PORT},HOSTSSL://0.0.0.0:{KAFKA_HOST_SSL_PORT},\
HOSTSASLSSL://0.0.0.0:{KAFKA_HOST_SASL_SSL_PORT},HOSTSASLPLAIN://0.0.0.0:{KAFKA_HOST_SASL_PLAIN_PORT},\
NETSASLSSL://0.0.0.0:{KAFKA_NETWORK_SASL_SSL_PORT}\n\
         advertised.listeners=INTER://localhost:19092,HOSTPLAIN://localhost:{plain},\
HOSTSSL://localhost:{ssl},HOSTSASLSSL://localhost:{sasl_ssl},HOSTSASLPLAIN://localhost:{sasl_plain},\
NETSASLSSL://{network_name}:{KAFKA_NETWORK_SASL_SSL_PORT}\n\
         listener.security.protocol.map=INTER:PLAINTEXT,CONTROLLER:PLAINTEXT,HOSTPLAIN:PLAINTEXT,\
HOSTSSL:SSL,HOSTSASLSSL:SASL_SSL,HOSTSASLPLAIN:SASL_PLAINTEXT,NETSASLSSL:SASL_SSL\n\
         sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512\n\
         ssl.keystore.type=PEM\n\
         ssl.keystore.location=/tmp/tls/broker.pem\n\
         ssl.client.auth=none\n\
         log.dirs=/tmp/kraft-logs\n\
         offsets.topic.replication.factor=1\n\
         transaction.state.log.replication.factor=1\n\
         transaction.state.log.min.isr=1\n\
         group.initial.rebalance.delay.ms=0\n\
         auto.create.topics.enable=true\n"
    );
    for listener in ["hostsaslssl", "hostsaslplain", "netsaslssl"] {
        properties.push_str(&format!(
            "listener.name.{listener}.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.\
PlainLoginModule required user_{username}=\"{password}\";\n\
             listener.name.{listener}.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.\
security.scram.ScramLoginModule required;\n\
             listener.name.{listener}.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.\
security.scram.ScramLoginModule required;\n"
        ));
    }
    let broker_pem = format!("{}{}", pki.server_key_pem, pki.server_cert_pem);
    let script = format!(
        "set -e; \
         /opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /tmp/tls/server.properties \
           --add-scram 'SCRAM-SHA-256=[name={username},password={password}]' \
           --add-scram 'SCRAM-SHA-512=[name={username},password={password}]'; \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/tls/server.properties"
    );
    let mut image = GenericImage::new("apache/kafka", "3.8.0")
        .with_exposed_port(KAFKA_HOST_PLAIN_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_entrypoint("/bin/sh")
        .with_cmd(["-c".to_string(), script])
        .with_copy_to("/tmp/tls/server.properties", properties.into_bytes())
        .with_copy_to("/tmp/tls/broker.pem", broker_pem.into_bytes())
        .with_mapped_port(plain, KAFKA_HOST_PLAIN_PORT.tcp())
        .with_mapped_port(ssl, KAFKA_HOST_SSL_PORT.tcp())
        .with_mapped_port(sasl_ssl, KAFKA_HOST_SASL_SSL_PORT.tcp())
        .with_mapped_port(sasl_plain, KAFKA_HOST_SASL_PLAIN_PORT.tcp());
    if let Some(network) = &placement.network {
        image = image.with_network(network.clone());
    }
    if let Some(name) = &placement.container_name {
        image = image.with_container_name(name.clone());
    }
    let container = image
        .start()
        .expect("start apache/kafka:3.8.0 (docker required)");
    crate::reap_on_exit(container.id());
    TlsKafkaBroker {
        container,
        plain_bootstrap: format!("localhost:{plain}"),
        ssl_bootstrap: format!("localhost:{ssl}"),
        sasl_ssl_bootstrap: format!("localhost:{sasl_ssl}"),
        sasl_ssl_port: sasl_ssl,
        sasl_plain_bootstrap: format!("localhost:{sasl_plain}"),
        username,
        password,
    }
}
