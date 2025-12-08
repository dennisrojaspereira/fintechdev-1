
# Use a stable OpenJDK base image; the former tag openjdk:21-jdk-slim was not found in the registry
FROM eclipse-temurin:21-jdk

ARG PG_JDBC_VERSION=42.7.3
RUN apt-get update \ 
	&& apt-get install -y --no-install-recommends curl \ 
	&& curl -L -o /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar \ 
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Main.java .

RUN javac -cp /tmp/postgresql.jar Main.java

EXPOSE 8080
CMD ["java", "-cp", "/app:/tmp/postgresql.jar", "Main"]
