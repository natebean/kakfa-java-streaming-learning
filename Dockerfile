FROM openjdk:8
RUN mkdir /usr/myapp
COPY target/StreamLearning-1.0-SNAPSHOT-standalone.jar /usr/myapp
WORKDIR /usr/myapp
ENTRYPOINT ["java", "-cp", "StreamLearning-1.0-SNAPSHOT-standalone.jar"]
CMD ["com.natebean.GapProductionLogSplitStream"]