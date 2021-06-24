for $i in json-file("src/test/resources/benchmark/datasets/git-archive.json")
let $key := $i.type
group by $key
return $key
