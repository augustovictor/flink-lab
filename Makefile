build-flink-image:
	docker build --tag pyflink:1.16.0 .

start-flink-container:
	docker-compose up

submit-job:
	docker exec flink-lab-jobmanager-1 ./bin/flink run -py /home/pyflink/main.py -pyexec /usr/local/bin/python -pyclientexec /usr/local/bin/python # -pyreq file:///home/pyflink/requirements.txt


