(:JIQS: ShouldRun; Output="({ "blender" : 250 }, { "broiler" : 20 }, { "shirt" : 10 }, { "socks" : 510 }, { "toaster" : 200 })" :)
for $sales in parallelize((
    { "product" : "broiler", "store number" : 1, "quantity" : 20  },
    { "product" : "toaster", "store number" : 2, "quantity" : 100 },
    { "product" : "toaster", "store number" : 2, "quantity" : 50 },
    { "product" : "toaster", "store number" : 3, "quantity" : 50 },
    { "product" : "blender", "store number" : 3, "quantity" : 100 },
    { "product" : "blender", "store number" : 3, "quantity" : 150 },
    { "product" : "socks", "store number" : 1, "quantity" : 500 },
    { "product" : "socks", "store number" : 2, "quantity" : 10 },
    { "product" : "shirt", "store number" : 3, "quantity" : 10 }
))
let $pname := $sales.product
group by $pname
order by $pname
return { $pname : sum($sales.quantity) }

(: test with no null or empty entries - grouping variable pre-defined with let :)
