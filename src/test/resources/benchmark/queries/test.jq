for $i in json-file("src/test/resources/benchmark/datasets/reddit.json")
let $key := $i.score
group by $key
return {"score": $key, "count": count($i)}