FROM openjdk:10-slim
MAINTAINER jgolda

ADD build/libs/averagePurchases.jar /app.jar
RUN set -o xtrace
CMD ["java", "-jar", "/app.jar", "com.github.serserser.kafka.etl.impl.AveragePurchasesInCountriesProcessingApp"]