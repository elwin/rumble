for $i in json-file("src/test/resources/benchmark/datasets/reddit_hetero.json")
let $key := $i.score
order by $key
return $key