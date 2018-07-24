FROM openjdk:10
MAINTAINER jgolda

ENV JAVA_OPTS=""
ADD build/libs/purchaseLoader.jar /app.jar
RUN set -o xtrace
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app.jar"]