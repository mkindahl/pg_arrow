create extension arrow;

create table test_heap (a int, b int);
create table test_arrow (like test_heap) using arrow;

insert into test_heap select a, 2 * a from generate_series(0,10) as a;
insert into test_arrow select * from test_heap;

select *
from test_arrow full join test_heap using (a,b)
where a is null or b is null;

drop table test_arrow, test_heap;
