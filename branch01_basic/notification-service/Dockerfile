FROM gradle:8.10.2-jdk21 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
RUN gradle dependencies --no-daemon -q 2>/dev/null || true
COPY src ./src
RUN gradle bootJar --no-daemon -q

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/build/libs/app.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]