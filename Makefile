format:
	mvn spotless:apply

test:
	mvn -Dtest=*Tests test

build:
	mvn clean compile assembly:single

benchmark:
	./benchmark.sh

copy_resources:
	rsync -r src/test/resources/benchmark rumble:~/rumble/src/test/resources

copy:
	rsync target/benchmark-rumble-jar-with-dependencies.jar rumble:~/rumble/target/benchmark-rumble-jar-with-dependencies.jar
	rsync benchmark.sh rumble:~/rumble

copy_results:
	rsync -r rumble:~/rumble/results .