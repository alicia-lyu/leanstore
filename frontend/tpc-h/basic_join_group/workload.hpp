#pragma once

// SELECT o.orderkey, o.orderdate, COUNT(*)
// FROM Orders o, Lineitem l
// WHERE o.orderkey = l.orderkey
// GROUP BY o.orderkey, o.orderdate;