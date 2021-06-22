FROM clojure:openjdk-15-alpine
RUN mkdir -p /usr/src/app /usr/src/app/resources
WORKDIR /usr/src/app
COPY project.clj /usr/src/app/
COPY resources /usr/src/app/resources/
COPY app-standalone.jar /usr/src/app/
CMD ["java", "-jar", "app-standalone.jar"]
