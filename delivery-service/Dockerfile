# Build context = repo root:  docker build -f delivery-service/Dockerfile .
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -B -pl delivery-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/delivery-service/target/delivery-service.jar /app/delivery-service.jar
ENV KAFKA_SECURITY=plaintext
ENTRYPOINT ["java", "-jar", "/app/delivery-service.jar"]
