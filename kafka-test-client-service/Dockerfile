# Build stage
FROM docker.io/library/maven:3-amazoncorretto-24-alpine AS builder

# Add non-root user for build
RUN addgroup -S builder && adduser -S builder -G builder
USER builder

# Set working directory
WORKDIR /workspace/app

# Cache Maven dependencies
COPY --chown=builder:builder kafka-test-client-service/pom.xml ./kafka-test-client-service/
COPY --chown=builder:builder pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code
COPY --chown=builder:builder kafka-test-client-service/src ./kafka-test-client-service/src/
COPY --chown=builder:builder kafka-test-client-service/pom.xml ./kafka-test-client-service/

# Build with optimizations
RUN mvn package -DskipTests \
    -Dmaven.test.skip=true \
    -Dmaven.javadoc.skip=true \
    -Dmaven.source.skip=true \
    --batch-mode \
    --quiet

# Runtime stage
FROM eclipse-temurin:17-jre-jammy AS runtime

# Add non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy JAR file from builder
COPY --from=builder --chown=appuser:appuser /workspace/app/kafka-test-client-service/target/*.jar app.jar

# Create configuration directory
RUN mkdir /config && chown appuser:appuser /config

# Set environment variables
ENV JAVA_OPTS="-XX:+UseContainerSupport \
    -XX:MaxRAMPercentage=75.0 \
    -XX:InitialRAMPercentage=50.0 \
    -XX:+OptimizeStringConcat \
    -XX:+UseStringDeduplication \
    -Djava.security.egd=file:/dev/./urandom"

# Set user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Expose ports
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]