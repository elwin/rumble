#!/bin/bash

EXE="target/benchmark-rumble-jar-with-dependencies.jar"
TYPES=("decimalgamma" "decimalgamma-loose" "default")
QUERIES=("reddit_2_o" "reddit_2_g" "reddit_2" "reddit_3_o" "reddit_3_g" "reddit_3" "reddit_o" "reddit_g" "reddit")
REPETITIONS=11

for QUERY in "${QUERIES[@]}"; do
  RESULT_PATH="results/${QUERY}.csv"

  echo "optimization,duration (ms)" >"$RESULT_PATH"

  for TYPE in "${TYPES[@]}"; do
    if [[ "${QUERY}" =~ ^git ]] && [ "${TYPE}" == "dataframe" ]; then
      continue
    fi

    if [[ "${QUERY}" == *_g ]] && [ "${TYPE}" == "decimalgamma-loose" ]; then
      continue
    fi

    echo "${QUERY} ${TYPE}"
    for ((i = 0; i < "${REPETITIONS}"; i++)); do
      DURATION=$(java -jar $EXE --type "${TYPE}" --query "src/test/resources/benchmark/queries/${QUERY}.jq" | xargs)
      echo "${TYPE},${DURATION}" >>"$RESULT_PATH"
    done

  done

done
