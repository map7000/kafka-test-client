#!/bin/bash
set -eo pipefail

# Configuration
KAFKA_CONTAINER="kafka-server"
APP_CONTAINER="kafka-test-app"
KAFKA_IMAGE="docker.io/apache/kafka-native:latest"
ALLURE_IMAGE="docker.io/frankescobar/allure-docker-service:latest"
NETWORK_NAME="kafka-net"
ALLURE_RESULTS="allure-results"
ALLURE_REPORT="allure-report"

# Cleanup function
cleanup() {
    echo "Cleaning up containers..."
    podman stop $KAFKA_CONTAINER $APP_CONTAINER 2>/dev/null || true
    podman rm $KAFKA_CONTAINER $APP_CONTAINER 2>/dev/null || true
    podman network rm $NETWORK_NAME 2>/dev/null || true
    echo "Cleanup complete"
}

# Register cleanup trap
trap cleanup EXIT

# 1. Create network
echo "Creating Podman network..."
podman network create $NETWORK_NAME

# 2. Start Kafka
echo "Starting Kafka container..."
podman run -d \
    --name $KAFKA_CONTAINER \
    --network $NETWORK_NAME \
    -p 9092:9092 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    $KAFKA_IMAGE

echo "Waiting for Kafka to initialize..."
sleep 20

# 3. Build and run Spring Boot app
echo "Building application..."
mvn clean install -pl kafka-test-client-service -am

echo "Building container image..."
podman build -t $APP_CONTAINER -f kafka-test-client-service/Dockerfile .

echo "Starting application container..."
podman run -d \
    --name $APP_CONTAINER \
    --network $NETWORK_NAME \
    -p 8080:8080 \
    $APP_CONTAINER

echo "Waiting for application to start..."
sleep 15

# 4. Run tests
echo "Executing tests..."
$MVN_CMD test -pl kafka-test-client-core -Dallure.results.directory=$ALLURE_RESULTS

# 5. Generate Allure report in container
echo "Generating Allure report..."
podman run --rm \
    -v ./$ALLURE_RESULTS:/allure-results \
    -v ./$ALLURE_REPORT:/allure-report \
    $ALLURE_IMAGE \
    allure generate /allure-results -o /allure-report --clean

echo "Allure report generated at: ./$ALLURE_REPORT/index.html"

# 6. Cleanup happens automatically via trap
echo "Pipeline completed successfully!"