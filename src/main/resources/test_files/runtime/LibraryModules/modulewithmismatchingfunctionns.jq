(:JIQS: ShouldNotParse; ErrorCode="XPST0003"; ErrorMetadata="LINE:6:COLUMN:0:" :)
module namespace my-module = "modulewithmismatchingfunctionns.jq";

declare function main($y) {
   my-module:func($my-module:x) + 2
};
declare variable $my-module:x := 2;

declare function my-module:func($y) {
   $y + 4
};
