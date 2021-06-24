for $i in json-file("src/test/resources/datasets/confusion/confusion-2014-03-02.json")
let $guess := $i.guess
group by $guess
return $guess
