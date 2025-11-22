# Use an OpenJDK 17 image
FROM eclipse-temurin:17-jdk-alpine

# Set working directory
WORKDIR /app

# Copy Gradle/Maven files (if you use Maven)
COPY pom.xml .
COPY src ./src

# Expose default Spring Boot port
EXPOSE 8080

# Build the app (for Maven)
RUN ./mvnw clean package -DskipTests || true

# Default command to run the jar
CMD ["sh", "-c", "java -jar target/*.jar"]