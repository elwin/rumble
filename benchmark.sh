EXE="target/benchmark-rumble-jar-with-dependencies.jar"
TYPES=("decimalgamma" "default")
QUERIES=("students" "confusion" "confusion_g" "confusion_o" "git" "git_g" "git_o")
REPETITIONS=5

for QUERY in "${QUERIES[@]}"; do
  RESULT_PATH="results/${QUERY}.csv"

  echo "optimization,duration (ms)" >"$RESULT_PATH"

  for TYPE in "${TYPES[@]}"; do
    if [[ "${QUERY}" =~ ^git ]] && [ "${TYPE}" == "dataframe" ]; then
      continue
    fi

    for ((i = 0; i < "${REPETITIONS}"; i++)); do
      DURATION=$(java -jar $EXE --type "${TYPE}" --file "src/test/resources/benchmark/queries/${QUERY}.jq" | xargs)
      echo "${TYPE},${DURATION}" >>"$RESULT_PATH"
    done

  done

done
