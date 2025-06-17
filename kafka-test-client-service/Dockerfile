# Build stage
FROM docker.io/library/maven/3-amazoncorretto-24-alpine AS builder

WORKDIR /workspace/app

# Copy only the files needed for dependency resolution (improves layer caching)
COPY kafka-test-client-service/pom.xml .
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source files
COPY kafka-test-client-service/src src/
COPY kafka-test-client-service/pom.xml .

# Build the application
RUN mvn package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /workspace/app/target/*.jar app.jar

# Set startup command
ENTRYPOINT ["java", "-jar", "app.jar"]