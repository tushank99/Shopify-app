# Build context is the REPO ROOT (see docker-compose.infra.yml), because the shared event contract
# lives in ../contracts and is copied onto the classpath by the Maven build.
#
#   docker build -f ledger-service/Dockerfile -t ledger-service .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# OpenTelemetry Java agent
ARG OTEL_JAVAAGENT_VERSION=2.33.1
RUN mkdir -p /otel \
 && curl -fsSL -o /otel/opentelemetry-javaagent.jar \
      "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_JAVAAGENT_VERSION}/opentelemetry-javaagent.jar"

COPY contracts ./contracts
COPY ledger-service/pom.xml ./ledger-service/pom.xml
COPY ledger-service/src ./ledger-service/src

RUN mvn -B -q -f ledger-service/pom.xml -DskipTests package

FROM eclipse-temurin:21-jre-alpine-3.24
WORKDIR /app

RUN adduser -D -u 1001 ledger

COPY --from=build /build/ledger-service/target/ledger-service.jar /app/ledger-service.jar
COPY --from=build /otel/opentelemetry-javaagent.jar /opt/otel/opentelemetry-javaagent.jar

USER ledger

# 8080 = API, 8081 = actuator
EXPOSE 8080 8081

ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -javaagent:/opt/otel/opentelemetry-javaagent.jar"

# Jaeger only ingests traces, so metrics and logs exporters are off.
ENV OTEL_SERVICE_NAME=ledger-service \
    OTEL_TRACES_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=none \
    OTEL_LOGS_EXPORTER=none \
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

ENTRYPOINT ["java", "-jar", "/app/ledger-service.jar"]