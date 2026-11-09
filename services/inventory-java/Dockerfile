# Java 21 / Spring Boot drop-in for the Node Inventory service (same port, env vars, topics and tables).
# Build: docker build -t skybook-inventory-java:latest services/inventory-java

# ---- build stage: dependencies are cached in their own layer, so code changes rebuild in seconds
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# ---- runtime stage: JRE only, non-root
FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home /app inventory
WORKDIR /app
COPY --from=build /src/target/inventory.jar /app/inventory.jar
USER 10001
EXPOSE 3003
# Container-aware heap sizing; the pod/compose memory limit decides the heap.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/inventory.jar"]
