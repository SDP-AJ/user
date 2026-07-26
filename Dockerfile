# Use platform-compatible JDK base image
FROM eclipse-temurin:17-jdk

# Set working directory inside container
WORKDIR /app

# Copy Maven build JAR into the image
COPY target/user-0.0.1-SNAPSHOT.jar app.jar

# Run the JAR
ENTRYPOINT ["java", "-jar", "app.jar"]
