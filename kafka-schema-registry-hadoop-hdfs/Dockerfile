FROM openjdk:11-jdk-slim AS builder

WORKDIR /app

COPY gradle gradle
COPY gradlew .
RUN chmod +x gradlew

COPY settings.gradle build.gradle ./
COPY src src

RUN ./gradlew clean bootJar -x test --no-daemon

FROM openjdk:11-jre-slim

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]