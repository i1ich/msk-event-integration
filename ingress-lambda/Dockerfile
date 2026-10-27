# Build context = repo root:  docker build -f ingress-lambda/Dockerfile .
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -B -pl ingress-lambda -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/ingress-lambda/target/ingress-lambda.jar /app/ingress-lambda.jar
ENV INGRESS_PORT=8080 KAFKA_SECURITY=plaintext
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/ingress-lambda.jar"]
