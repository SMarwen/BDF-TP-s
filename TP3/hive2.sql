set hive.cli.print.header=true;
set hive.exec.dynamic.partition=true;

SELECT prenom, count(nbo)
FROM msprenoms
LATERAL VIEW explode(split(lower(origin), ',[ ]*')) t1 as nbo
group by prenom;
