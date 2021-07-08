for $i in json-file("src/test/resources/benchmark/datasets/reddit_2.json")
let $key := $i.score
order by $key
return $key