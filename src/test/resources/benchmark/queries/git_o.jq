for $i in json-file("src/test/resources/benchmark/datasets/git-archive.json")
let $key := $i.actor.id
return {"actor": $key, "count": count($i)}