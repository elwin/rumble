for $i in json-file("src/test/resources/benchmark/datasets/reddit.json")
let $key := $i.score
order by $key
return $key