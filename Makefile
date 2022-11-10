up:
	docker-compose up --build --force-recreate -V
	# docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/my-topic
	# docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/my-topic
	# docker exec pulsar bin/pulsar-admin topics get-retention persistent://public/default/my-topic
	# docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/results
	# docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/results

create-pulsar-topics:
	docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/my-topic
	docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/my-topic
	docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/results
	docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/results
	# @if [[ $$(docker exec pulsar bin/pulsar-admin topics get-retention persistent://public/default/my-topic) == *"Topic not found" ]]; then\
	# 	docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/my-topic &&\
	# 	docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/my-topic &&\
	# 	docker exec pulsar bin/pulsar-admin topics get-retention persistent://public/default/my-topic;\
	# else\
	# 	echo "Topic 'my-topic' already exists";\
	# fi
	# @if [[ $$(docker exec pulsar bin/pulsar-admin topics get-retention persistent://public/default/results) == *"Topic not found" ]]; then\
	# 	docker exec pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/results &&\
	# 	docker exec pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/results &&\
	# 	docker exec pulsar bin/pulsar-admin topics get-retention persistent://public/default/results;\
	# else\
	# 	echo "Topic 'results' already exists";\
	# fi

	@docker exec pulsar bin/pulsar-admin topics list-partitioned-topics public/default

flink-build-image:
	docker build --tag pyflink:1.16.0 .

flink-start-container:
	docker-compose up jobmanager taskmanager

flink-submit-job:
	docker exec flink-lab-jobmanager-1 ./bin/flink run -py /home/pyflink/main.py -pyexec /usr/local/bin/python -pyclientexec /usr/local/bin/python # -pyreq file:///home/pyflink/requirements.txt

flink-submit-job:

	# https://mvnrepository.com/artifact/org.apache.flink/flink-connector-pulsar/1.16.0
	# https://streamnative.io/blog/release/2022-09-29-announcing-the-flink-pulsar-sql-connector/
	# https://repo1.maven.org/maven2/io/streamnative/connectors/flink-sql-connector-pulsar/1.15.1.1/
	docker exec flink-lab-jobmanager-1 ./bin/flink run -py /home/pyflink/flink/consumer.py -pyexec /usr/local/bin/python -pyclientexec /usr/local/bin/python #--pyfiles file:///home/pyflink/flink-connector-pulsar-1.16.0.jar --jarfile file:///home/pyflink/flink-connector-pulsar-1.16.0.jar

flink-ls-jobmanager-pyflink-folder:
	docker exec flink-lab-jobmanager-1 ls /home/pyflink

pulsar-list-topics:
	docker exec pulsar bin/pulsar-admin topics list public/default

pulsar-produce-messages:
	poetry run python pulsar/producer.py

pulsar-consume-messages:
	poetry run python pulsar/consumer.py


