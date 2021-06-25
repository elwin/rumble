EXE="target/benchmark-rumble-jar-with-dependencies.jar"

TYPES=("default" "decimalgamma" "dataframe")

for TYPE in "${TYPES[@]}"
do
  echo "${TYPE}"
  java -jar $EXE --type "${TYPE}" --file src/test/resources/benchmark/queries/students.jq > out
done

