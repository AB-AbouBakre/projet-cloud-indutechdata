FROM apache/spark:4.2.0-python3

USER root
COPY consumer.py /opt/spark/work-dir/consumer.py
RUN chmod 0444 /opt/spark/work-dir/consumer.py
USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--conf", "spark.jars.ivy=/tmp/.ivy2", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", "/opt/spark/work-dir/consumer.py"]
