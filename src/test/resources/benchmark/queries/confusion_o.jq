for $i in json-file("src/test/resources/benchmark/datasets/confusion.json")
let $key := date($i.date)
order by $key
return $key