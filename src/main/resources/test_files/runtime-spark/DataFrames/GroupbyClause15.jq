(:JIQS: ShouldRun; Output="2" :)
for $i in json-file("./src/main/resources/queries/conf-ex.json")
group by $y := $i.country, $t := $i.target
where count($i) eq 2
return count($i)