# Build context = repo root:  docker build -f stream-processor/Dockerfile .
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -B -pl stream-processor -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/stream-processor/target/stream-processor.jar /app/stream-processor.jar
ENV KAFKA_SECURITY=plaintext
ENTRYPOINT ["java", "-jar", "/app/stream-processor.jar"]
