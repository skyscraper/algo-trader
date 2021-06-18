FROM clojure:openjdk-15-alpine
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj /usr/src/app/
COPY app-standalone.jar /usr/src/app/
CMD ["java", "-jar", "app-standalone.jar"]
