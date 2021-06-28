for $i in json-file("src/test/resources/benchmark/datasets/git-archive.json")
let $key := $i.actor.login
order by $key
return {"actor": $key, "count": count($i)}