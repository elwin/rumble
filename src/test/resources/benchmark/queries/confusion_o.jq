for $i in json-file("src/test/resources/benchmark/datasets/confusion/confusion-2014-03-02.json")
let $key := $i.date
order by $key
return $key