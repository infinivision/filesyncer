# 安装数据库

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
SELECT get_shuke_stats(2, '2019-04-18T00:00:00+08:00', 'now');

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

# 运维手册

## 启动、停止后端服务
```bash
abclab@cpu01:~$ cat start_faceserver.sh
#!/bin/bash
start-stop-daemon --start --background --no-close --make-pidfile --pidfile /tmp/backend.pid --exec /usr/bin/java -- -jar /opt/apps/iot/backend/target/iot-backend-1.0.0.jar > /home/abclab/backend.log 2>&1

start-stop-daemon --start --background --no-close --make-pidfile --pidfile /tmp/wss.pid --exec /opt/apps/iot/terms/wss -- --addr-http=:9094 --addr-metric=:9095 --log-file=/opt/apps/iot/terms/wss.log

start-stop-daemon --start --background --no-close --make-pidfile --pidfile /tmp/eureka.pid --exec /usr/bin/java -- -jar /opt/apps/iot/eureka-server/iot-eureka-1.0.0.jar > /home/abclab/eureka.log 2>&1

start-stop-daemon --start --background --no-close --make-pidfile --pidfile /tmp/service-analysis.pid --exec /usr/bin/java -- -jar /opt/apps/analysis/service-analysis.jar -P prod > /home/abclab/analysis.log 2>&1

start-stop-daemon --start --background --no-close --make-pidfile --pidfile /tmp/faceserver.pid --exec  /home/abclab/faceserver -- --redis-addr 172.19.0.16:30379 --dest-pg-url postgres://mcd:mcd_hello_pg@172.19.0.108:5432/mcd --identify-distance-threshold2 0.6 --identify-distance-threshold3 0.8 --addr-pprof=:8091 --addr 172.19.0.101:8090 --metric-addr 172.19.0.101:8002 --addr-oss 172.19.0.103 --oss-bucket images --oss-key HYR0DJYTENCIL2HYAMZ1 --oss-secret-key KQpl95qvokPktKZ1xpRovTRVQGe6k7szG0UfVa6r --predict-serv-url http://172.19.0.16:30080/ --eureka-addr http://172.19.0.101:8761/eureka --log-level=debug --log-file=/home/abclab/faceserver.log


abclab@cpu01:~$ cat stop_faceserver.sh
#!/bin/bash
start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/backend.pid
start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/wss.pid
start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/eureka.pid
start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/service-analysis.pid
start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile /tmp/faceserver.pid

```

## 捞图片
在发现人脸推理模型的问题（同一人被识别为多个用户ID、多个人被识别为同一用户ID等）后，常常需要提取某断时间范围、指定用户ID的图片用于诊断问题。
zhichyu@ak47:~/go/src/github.com/infinivision/filesyncer$ cmd/fetchImgs/fetchImgs --redis-addr 172.19.0.16:30379 --addr-oss 172.19.0.103 --oss-bucket images --oss-key HYR0DJYTENCIL2HYAMZ1 --oss-secret-key KQpl95qvokPktKZ1xpRovTRVQGe6k7szG0UfVa6r --output=/home/zhichyu/Downloads --date-start 2019-04-16T09:30:00+08:00 --date-end 2019-04-16T09:35:00+08:00


## 更换人脸推理模型
更换人脸推理模型，从一个图片infer的embedding发生变化，导致hyena人脸数据库过时。所以需要清理后端系统(hyena, cell, citus)所有数据，利用cell内持久化的访问记录（记录了每个图片对应的店铺ID、图片ID、用户ID、时间戳等）列表重建hyena, cell, citus。
- 1 停止faceserver
abclab@cpu01:~$ pkill faceserver

- 2 备份cell中的访问记录
zhichyu@ak47:~/go/src/github.com/infinivision/filesyncer$ cmd/cpRedisQue/cpRedisQue

- 3 备份数据库
pg_dump --host 172.19.0.108 --username=postgres --no-password --dbname=mcd --clean > dump.sql
psql --host 172.19.0.108 --username=postgres --no-password -f dump.sql 

- 4 清理数据库
psql --host 172.19.0.108 --username=postgres --no-password -f create_database_mcd.sql 

- 5 清理hyena和cell

- 6 使用faceserver重放访问记录
abclab@cpu01:~$ /home/abclab/faceserver --addr-replay 172.19.0.101:6379 --redis-addr 172.19.0.16:30379 --identify-distance-threshold2 0.6 --identify-distance-threshold3 0.8 --addr-pprof=:8091 --addr 172.19.0.101:8090 --metric-addr 172.19.0.101:8002 --addr-oss 172.19.0.103 --oss-bucket images --oss-key HYR0DJYTENCIL2HYAMZ1 --oss-secret-key KQpl95qvokPktKZ1xpRovTRVQGe6k7szG0UfVa6r --predict-serv-url http://172.19.0.16:30080/ --eureka-addr http://172.19.0.101:8761/eureka --log-level=debug --log-file=/home/abclab/faceserver.log

注：可利用faceserver的选项--replay-date-start，--replay-date-end重放指定时间范围的图片。

- 7 启动faceserver
abclab@cpu01:~$ bash start_faceserver.sh

- 8 更新数据库中的统计表
```postgresql
mcd=# SELECT restore_visit_stats();
mcd=# SELECT update_frequent_users();
```

## 重建数据库citus
如果citus数据丢失全部或部分，可以从利用cell内持久化的访问记录列表重建citus。
- 1 停止faceserver
abclab@cpu01:~$ pkill faceserver

- 2 清理数据库
psql --host 172.19.0.108 --username=postgres --no-password -f create_database_mcd.sql 

- 3 使用faceserver重放访问记录
zhichyu@ak47:~/go/src/github.com/infinivision/filesyncer/cmd/replayVisits$ ./replayVisits --redis-addr 172.19.0.101:6379 --dest-pg-url postgres://mcd:mcd_hello_pg@172.19.0.108:5432/mcd

注：可利用replayVisits的选项--replay-date-start，--replay-date-end重放指定时间范围的图片。

- 4 启动faceserver
abclab@cpu01:~$ bash start_faceserver.sh

- 5 更新数据库中的统计表
```postgresql
mcd=# SELECT restore_visit_stats();
mcd=# SELECT update_frequent_users();
```
