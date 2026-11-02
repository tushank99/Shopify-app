# Build context is the REPO ROOT (see docker-compose.infra.yml), because the shared event contract
# lives in ../contracts and is copied onto the classpath by the Maven build.
#
#   docker build -f ledger-service/Dockerfile -t ledger-service .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY contracts ./contracts
COPY ledger-service/pom.xml ./ledger-service/pom.xml
COPY ledger-service/src ./ledger-service/src
RUN mvn -B -q -f ledger-service/pom.xml -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 1001 ledger
COPY --from=build /build/ledger-service/target/ledger-service.jar /app/ledger-service.jar
USER ledger

# 8080 = API (published on loopback only by compose), 8081 = actuator (never published).
EXPOSE 8080 8081
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "/app/ledger-service.jar"]