for $i in json-file("src/test/resources/benchmark/datasets/students.json")
let $key := $i.university
group by $key
return {"school": $key, "count": count($i)}
