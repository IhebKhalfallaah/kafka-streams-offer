# ===== BUILD STAGE =====
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app

# copy pom + sources
COPY pom.xml .
COPY src ./src

# build jar
RUN mvn -DskipTests clean package


# ===== RUNTIME STAGE =====
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# copy the built jar (any name) to app.jar
COPY --from=build /app/target/*.jar app.jar

# config via env
ENV SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
ENV SERVER_PORT=8088

EXPOSE 8088

ENTRYPOINT ["java","-jar","/app/app.jar"]
