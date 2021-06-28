EXE="target/benchmark-rumble-jar-with-dependencies.jar"
TYPES=("default" "decimalgamma" "dataframe")
QUERIES=("students" "confusion" "confusion_g" "confusion_o")

for QUERY in "${QUERIES[@]}"; do
  RESULT_PATH="results/${QUERY}"

  echo "type,duration (ms)" >"$RESULT_PATH"

  for TYPE in "${TYPES[@]}"; do
    DURATION=$(java -jar $EXE --type "${TYPE}" --file "src/test/resources/benchmark/queries/${QUERY}.jq" | xargs)
    echo "${TYPE},${DURATION}" >>"$RESULT_PATH"
  done

done
