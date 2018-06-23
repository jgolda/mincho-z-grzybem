FROM openjdk:10-slim
MAINTAINER jgolda

ENV JAVA_OPTS=""
ADD build/libs/totalPurchases.jar /app.jar
RUN set -o xtrace
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app.jar"]