FROM maven:latest
COPY src/ src
COPY pom.xml .
RUN mvn compile
CMD ["mvn", "exec:java"]