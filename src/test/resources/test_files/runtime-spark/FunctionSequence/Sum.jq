(:JIQS: ShouldRun; Output="(0, 9, 500500, 500509, P1000Y, P1000Y, 5.0005E7, 0, 1)" :)
sum(parallelize(())),
sum(parallelize(()), 1),
sum(parallelize(1 to 1000)),
sum(parallelize(1 to 1000), 1),
sum(parallelize(1 to 1000) ! yearMonthDuration("P1Y"), yearMonthDuration("P0Y")),
sum((1 to 1000) ! yearMonthDuration("P1Y"), yearMonthDuration("P0Y")),
sum(annotate(parallelize(1 to 10000) ! { "foo" : $$ cast as double }, { "foo" : "double" }).foo),
sum(annotate(parallelize(1 to 10000) ! { "foo" : $$ cast as double }, { "foo" : "double" }).bar),
sum(annotate(parallelize(1 to 10000) ! { "foo" : $$ cast as double }, { "foo" : "double" }).bar, 1)
