(:JIQS: ShouldRun; Output="(1, 1, 2.2, 3)" :)
for $i in parallelize((3, 2.2, 1, 1))
order by $i
return $i
