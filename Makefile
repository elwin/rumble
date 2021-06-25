format:
	mvn spotless:apply

test:
	mvn -Dtest=*Tests test

build:
	mvn clean compile assembly:single

benchmark:
	./benchmark.sh