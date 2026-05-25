# Stage 1: Build stage
FROM maven:3.9.5-eclipse-temurin-21-alpine AS builder
WORKDIR /app
COPY pom.xml .
# Prefetch dependencies to build faster on rebuilds
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Runtime stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/data-export-engine-1.0.0.jar app.jar

# Expose server port
EXPOSE 8080

# Configure JVM for strict 256m container memory limit
# -Xmx128m caps the heap at 128MB, leaving ample space for Metaspace, threads, and container overhead.
ENV JAVA_OPTS="-Xmx128m -Xms128m -XX:MaxMetaspaceSize=64m -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
