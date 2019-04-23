# Database setup

## Install Citus & other extensions
Refers to [Citus Docs](https://docs.citusdata.com/en/v8.1/installation/multi_machine_rhel.html).
Note: Log files are under /var/lib/pgsql/11/data/log/.
```bash
$ sudo yum install citus81_11
$ sudo yum install postgresql11-devel llvm-toolset-7 llvm5.0 protobuf-c-devel pg_cron_11

$ export PATH=$PATH:/usr/pgsql-11/bin

$ git clone https://github.com/ChenHuajun/pg_roaringbitmap.git
$ pushd pg_roaringbitmap && sudo make install && popd

$ git clone https://github.com/citusdata/postgresql-hll.git
$ pushd postgresql-hll && sudo make install && popd

$ git clone https://github.com/pgpartman/pg_partman.git
$ pushd pg_partman && sudo make install && popd

$ git clone https://github.com/citusdata/cstore_fdw.git
$ pushd cstore_fdw && sudo make install && popd

$ sudo vim /var/lib/pgsql/11/data/postgresql.conf
# Add settings for extensions here
shared_preload_libraries = 'citus, pg_cron, hll, pg_partman_bgw, cstore_fdw'

$ sudo vim /var/lib/pgsql/11/data/pg_hba.conf
# Comment out following lines to trust local connections.
#host    all             all             127.0.0.1/32            ident
#host    all             all             ::1/128                 ident

$ sudo systemctl restart postgresql-11
```

## Create role "mcd" on each node of citus cluster
Login postgresql as superuser & default database, do the following:
```postgresql
CREATE ROLE mcd LOGIN PASSWORD 'XXX';
```

## Create database "mcd" on coordinator
Login postgresql as superuser & default database, do the following:
```bash
$ psql --host xx.xx.xx.xx --username=xx.xx -f create_database_mcd.sql
```

## Some SQL statements frequently used
```postgresql
SELECT quality FROM users WHERE uid=20 ORDER BY quality DESC LIMIT 10;
SELECT count(*), min(sorted.quality) FROM (SELECT quality FROM users WHERE uid=20 ORDER BY quality DESC LIMIT 10) sorted;
SELECT uid, max(quality) FROM users GROUP BY uid ORDER BY uid;
SELECT uid, count(*) FROM users GROUP BY uid;
SELECT gender, age FROM users WHERE uid=148 ORDER BY quality LIMIT 1;
SELECT insert_user(2868, 'b98478388837496aade39745576dcaa8', 0.9, 1, 55, timestamptz '2019-03-20T09:40:11+08:00');

SELECT * FROM visit_events ORDER BY visit_time DESC LIMIT 3;
SELECT distinct(uid) FROM visit_events ORDER BY uid;
SELECT distinct(uid) FROM users ORDER BY uid;
SELECT distinct(uid) FROM visit_events WHERE uid NOT IN (SELECT distinct(uid) FROM users ORDER BY uid) ORDER BY uid;
SELECT count(*) FROM visit_events WHERE uid IN (SELECT distinct(uid) FROM visit_events WHERE uid NOT IN (SELECT distinct(uid) FROM users ORDER BY uid) ORDER BY uid);
SELECT max(visit_time) FROM visit_events WHERE uid IN (SELECT distinct(uid) FROM visit_events WHERE uid NOT IN (SELECT distinct(uid) FROM users ORDER BY uid) ORDER BY uid);
SELECT insert_visit_event(2, 15, 0, 0, 50, timestamptz '2019-03-20T09:40:11+08:00');

SELECT * FROM visit_stats_user ORDER BY shop_id, uid;
SELECT * FROM (SELECT uid, rb_cardinality(hours) AS count, rb_range_cardinality(hours,431728,431863) AS rangeCount FROM visit_stats_user WHERE shop_id=2 ORDER BY count DESC) foo WHERE rangeCount>0;

SELECT shop_id, precision, visit_time, rb_cardinality(frequent_users) AS frequent_users, rb_cardinality(total) AS total FROM visit_stats_uv ORDER BY shop_id, precision, visit_time;
DELETE FROM visit_stats_uv WHERE rb_cardinality(frequent_users)=0 AND rb_cardinality(total)=0;
UPDATE visit_stats_uv SET frequent_users='{}'::roaringbitmap;
SELECT shop_id, precision, visit_time, rb_cardinality(frequent_users) AS frequent_users, rb_cardinality(total) AS total FROM visit_stats_uv ORDER BY shop_id, precision, visit_time;
SELECT visit_time, rb_andnot_cardinality(total, frequent_users) AS first_users, rb_and_cardinality(total, frequent_users) AS multi_users FROM visit_stats_uv WHERE shop_id=2 AND precision='day' ORDER BY visit_time;
SELECT rb_cardinality(rb_or_agg(total)) FROM (SELECT total FROM visit_stats_uv WHERE shop_id=2 AND precision='day' AND visit_time>=timestamptz 'now' - interval '40 days' AND visit_time<timestamptz 'now' - interval '10 days' ORDER BY visit_time) foo;

SELECT * from cron.job;
SELECT cron.unschedule(1);

SELECT date_trunc('day', visit_time) AS date, count(*) FROM visit_events_extra GROUP BY date ORDER BY date;
SELECT * FROM visit_events WHERE date_trunc('day', visit_time)=timestamptz '2019-04-18T00:00:00+08:00' ORDER BY visit_time;

SELECT uid FROM visit_events WHERE date_trunc('day', visit_time)=timestamptz '2019-04-18T00:00:00+08:00' GROUP BY uid;
SELECT uid FROM users GROUP BY uid;
SELECT count(*) FROM
	(SELECT uid FROM visit_events WHERE date_trunc('day', visit_time)=timestamptz '2019-04-18T00:00:00+08:00' GROUP BY uid) foo
	WHERE NOT uid IN (SELECT uid FROM users GROUP BY uid);

```
