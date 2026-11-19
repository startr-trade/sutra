# IBM MQ Advanced for Developers with its AMQP 1.0 component, for the AMQP transport's IBM MQ
# suite. The published developer image leaves the AMQP component out; this adds it back from the
# developer archive of the same release. build-image.sh prepares the context and builds it.
ARG MQ_IMAGE=icr.io/ibm-messaging/mq:10.0.1.0-r1
FROM ${MQ_IMAGE}
COPY --chown=1001:0 amqp/ /opt/mqm/amqp/
