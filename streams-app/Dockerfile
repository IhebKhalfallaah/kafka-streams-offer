# ===== BUILD STAGE =====
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app

COPY pom.xml .

COPY src ./src

RUN mvn -q -DskipTests package

# ===== RUNTIME IMAGE =====
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# copy the shaded (dependency-bundled) jar
COPY --from=build /app/target/streams-app-1.0.0.jar app.jar

# default bootstrap for containers; override in compose if needed
ENV KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092

# we’ll override the command per service in docker-compose
ENTRYPOINT ["java","-cp","/app/app.jar"]
