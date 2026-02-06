.PHONY: run dashboard poller test docker-build docker-up docker-down

run:
	python3 -m pequod

dashboard:
	python3 -m pequod dashboard

poller:
	python3 -m pequod poller

test:
	python3 -m unittest discover -s tests

docker-build:
	docker build -t pequod:latest .

docker-up:
	docker compose up --build

docker-down:
	docker compose down

