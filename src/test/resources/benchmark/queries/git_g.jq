for $i in json-file("src/test/resources/benchmark/datasets/git-archive.json")
let $key := $i.actor.login
group by $key
return {"actor": $key, "count": count($i)}