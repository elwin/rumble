for $i in json-file("src/test/resources/benchmark/datasets/reddit_3.json")
let $key := $i.score
group by $key
order by $key
return {"score": $key, "count": count($i)}