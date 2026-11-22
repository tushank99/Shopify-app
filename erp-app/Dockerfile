# Build context is the REPO ROOT (the multi-module reactor needs the root pom.xml and every module).
#
#   docker build -f erp-app/Dockerfile -t erp-service .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml ./pom.xml
COPY contracts ./contracts
# ledger-service is a reactor module, so its pom must exist; -pl erp-app -am builds only what the ERP needs.
COPY ledger-service/pom.xml ./ledger-service/pom.xml
COPY erp-common ./erp-common
COPY erp-inventory ./erp-inventory
COPY erp-purchasing ./erp-purchasing
COPY erp-orders ./erp-orders
COPY erp-app ./erp-app

RUN mvn -B -q -pl erp-app -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine-3.24
WORKDIR /app
RUN adduser -D -u 1001 erp
COPY --from=build /build/erp-app/target/erp-service.jar /app/erp-service.jar
USER erp
EXPOSE 8095
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "/app/erp-service.jar"]
