-- https://dba.stackexchange.com/questions/11893/force-drop-db-while-others-may-be-connected
ALTER DATABASE mcd CONNECTION LIMIT 0;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'mcd';
DROP DATABASE IF EXISTS mcd;
CREATE DATABASE mcd;

CREATE EXTENSION IF NOT EXISTS pg_cron;
DELETE FROM cron.job WHERE database='mcd';
INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username) VALUES ('0 0 * * *', 'SELECT update_frequent_users()', 'localhost', 5432, 'mcd', 'postgres');
INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username) VALUES ('0 0 0 * *', 'SELECT forget_old_visits()', 'localhost', 5432, 'mcd', 'postgres');

\connect mcd;
CREATE EXTENSION roaringbitmap;
CREATE EXTENSION hll;

CREATE TABLE visit_events (
	uid int8 NOT NULL, 
	visit_time timestamptz NOT NULL, 
	shop_id int8 NOT NULL, 
	position int NOT NULL, 
	duration int DEFAULT 0,
	PRIMARY KEY(uid, visit_time)
);

CREATE TABLE users (
	picture_id text NOT NULL, 
	quality float NOT NULL, 
	uid int8 NOT NULL, 
	age int NOT NULL, 
	gender int NOT NULL, 
	visit_time timestamptz NOT NULL, 
	PRIMARY KEY(picture_id)
);

CREATE TABLE visit_stats_user (
	shop_id int8 NOT NULL,
	uid int8 NOT NULL,
	hours roaringbitmap NOT NULL,
	last_visit_time timestamptz NOT NULL,
	PRIMARY KEY(shop_id, uid)
);

CREATE TABLE visit_stats_pv (
	shop_id int8 NOT NULL, 
	precision text NOT NULL, -- hour, day, month, year
	visit_time timestamptz NOT NULL, -- round to given precision
	total int DEFAULT 0, -- ts+uid occurred at the given precision
	gender_0 int DEFAULT 0, 
	gender_1 int DEFAULT 0, 
	age_0 int DEFAULT 0, 
	age_1 int DEFAULT 0, 
	age_2 int DEFAULT 0, 
	age_3 int DEFAULT 0, 
	age_4 int DEFAULT 0, 
	age_5 int DEFAULT 0, 
	age_6 int DEFAULT 0, 
	age_7 int DEFAULT 0, 
	age_8 int DEFAULT 0, 
	age_9 int DEFAULT 0, 
	age_10 int DEFAULT 0, 
	age_11 int DEFAULT 0, 
	age_12 int DEFAULT 0, 
	age_13 int DEFAULT 0, 
	age_14 int DEFAULT 0, 
	age_15 int DEFAULT 0, 
	age_16 int DEFAULT 0, 
	age_17 int DEFAULT 0, 
	age_18 int DEFAULT 0, 
	age_19 int DEFAULT 0, 
	PRIMARY KEY(shop_id, precision, visit_time)
);

CREATE TABLE visit_stats_uv (
	shop_id int8 NOT NULL, 
	precision text NOT NULL, -- day, month
	visit_time timestamptz NOT NULL, -- round to given precision
	frequent_users roaringbitmap DEFAULT '{}'::roaringbitmap, -- users before the given visit_time, updated by a cron job
	total roaringbitmap DEFAULT '{}'::roaringbitmap, -- users occurred at the given visit_time (including)
	gender_0 roaringbitmap DEFAULT '{}'::roaringbitmap,
	gender_1 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_0 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_1 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_2 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_3 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_4 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_5 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_6 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_7 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_8 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_9 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_10 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_11 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_12 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_13 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_14 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_15 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_16 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_17 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_18 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	age_19 roaringbitmap DEFAULT '{}'::roaringbitmap, 
	PRIMARY KEY(shop_id, precision, visit_time)
);

CREATE INDEX users_idx ON users (uid, quality);

CREATE INDEX cardinality_idx ON visit_stats_user (shop_id, rb_cardinality(hours) DESC);

CREATE OR REPLACE FUNCTION clear_visits() RETURNS int AS $$
DECLARE
BEGIN
	DELETE FROM users;
	DELETE FROM visit_events;
	DELETE FROM visit_stats_user;
	DELETE FROM visit_stats_pv;
	DELETE FROM visit_stats_uv;
	RETURN 1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_user(param_uid int8, param_picture_id text, param_quality float, param_gender int, param_age int, param_visit_time timestamptz) RETURNS int AS $$
DECLARE
	num_keep int = 10;
	val_cnt int;
	val_quality float;
BEGIN
	SELECT count(*), min(sorted.quality) INTO val_cnt, val_quality FROM (SELECT quality FROM users WHERE uid=param_uid ORDER BY quality DESC LIMIT num_keep) sorted;
	IF val_cnt < num_keep THEN
		INSERT INTO users(uid, picture_id, quality, age, gender, visit_time) VALUES (param_uid, param_picture_id, param_quality, param_age, param_gender, param_visit_time) ON CONFLICT (picture_id) DO NOTHING;
		RETURN 1;
	ELSIF param_quality > val_quality THEN
		DELETE FROM users WHERE uid=param_uid AND quality<=val_quality;
		INSERT INTO users(uid, picture_id, quality, age, gender, visit_time) VALUES (param_uid, param_picture_id, param_quality, param_age, param_gender, param_visit_time) ON CONFLICT (picture_id) DO NOTHING;
		RETURN 2;
	END IF;
	RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_visit_event(param_shop_id int8, param_uid int8, param_position int, param_gender int, param_age int, param_visit_time timestamptz) RETURNS int AS $$
DECLARE
	val_ts timestamptz;
BEGIN
	IF param_position = 0 THEN
		SELECT last_visit_time INTO val_ts FROM visit_stats_user WHERE shop_id=param_shop_id AND uid=param_uid;
		IF NOT FOUND THEN
			val_ts = 'epoch';
		END IF;
		IF param_visit_time + interval '1 minutes' <= val_ts OR param_visit_time >= val_ts + interval '30 minutes' THEN
			INSERT INTO visit_events(shop_id, uid, position, visit_time, duration) VALUES (param_shop_id, param_uid, param_position, param_visit_time, 0) ON CONFLICT (uid, visit_time) DO NOTHING;
   	    	PERFORM insert_visit_stats(param_shop_id, param_uid, param_gender, param_age, param_visit_time);
			RETURN 1;
		END IF;
	ELSIF param_position = 1 THEN
		SELECT max(visit_time) INTO val_ts FROM visit_events WHERE shop_id=param_shop_id AND uid=param_uid AND position=0 AND visit_time<=param_visit_time;
		IF val_ts IS NULL THEN
			RETURN 0;
		END IF;
		UPDATE visit_events SET duration=GREATEST(duration, EXTRACT(EPOCH FROM param_visit_time) - EXTRACT(EPOCH FROM val_ts)) WHERE shop_id=param_shop_id AND uid=param_uid AND position=0 AND visit_time=val_ts;
		RETURN 1;
	ELSE
		INSERT INTO visit_events(shop_id, uid, position, visit_time, duration) VALUES (param_shop_id, param_uid, param_position, param_visit_time, 0) ON CONFLICT (uid, visit_time) DO NOTHING;
		RETURN 1;
	END IF;
	RETURN 0;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_visit_stats(param_shop_id int8, param_uid int8, param_gender int, param_age int, param_visit_time timestamptz) RETURNS int AS $$
DECLARE
	precisions1 text[] := array['hour', 'day', 'month', 'year'];
	precisions2 text[] := array['day', 'month'];
	val_template1 text := 'INSERT INTO visit_stats_pv AS v(shop_id, precision, visit_time, total, %1$s, %2$s) VALUES ($1, $2, $3, 1, 1, 1) ON CONFLICT (shop_id, precision, visit_time) DO UPDATE SET total=v.total+1, %1$s=v.%1$s+1, %2$s=v.%2$s+1';
	val_template2 text := 'INSERT INTO visit_stats_uv AS v(shop_id, precision, visit_time, total, %1$s, %2$s) VALUES ($1, $2, $3, $4, $4, $4) ON CONFLICT (shop_id, precision, visit_time) DO UPDATE SET total=EXCLUDED.total|v.total, %1$s=EXCLUDED.%1$s|v.%1$s, %2$s=EXCLUDED.%2$s|v.%2$s';
	val_precision text;
	tag_gender text;
	tag_age text;
	val_sql text;
	val_hour int;
BEGIN
	val_hour = EXTRACT(EPOCH FROM param_visit_time)/(60*60);
	INSERT INTO visit_stats_user AS v(shop_id, uid, hours, last_visit_time) VALUES (param_shop_id, param_uid, rb_build(ARRAY[val_hour]), param_visit_time) ON CONFLICT (shop_id, uid) DO UPDATE SET hours=EXCLUDED.hours|v.hours, last_visit_time=EXCLUDED.last_visit_time;
	IF param_gender = 0 THEN
		tag_gender = 'gender_0';
	ELSE
		tag_gender = 'gender_1';
	END IF;
	IF param_age >= 100 THEN
		tag_age = 'age_19';
	ELSIF param_age < 0 THEN
		tag_age = 'age_0';
	ELSE
		tag_age = 'age_' || (param_age/5)::text;
	END IF;
	val_sql = format(val_template1, tag_gender, tag_age);
	FOREACH val_precision IN ARRAY precisions1 LOOP 
		EXECUTE val_sql USING param_shop_id, val_precision, date_trunc(val_precision, param_visit_time);
	END LOOP;
	val_sql = format(val_template2, tag_gender, tag_age);
	FOREACH val_precision IN ARRAY precisions2 LOOP 
		EXECUTE val_sql USING param_shop_id, val_precision, date_trunc(val_precision, param_visit_time), rb_build(ARRAY[param_uid::int]);
	END LOOP;
	RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_frequent_users() RETURNS int AS $$
DECLARE
	precisions text[] := array['day', 'month'];
	prec_intervals interval[] := array[interval '1 day', interval '1 month'];
    val_ret int := 0;
    val_i int;
    val_cnt int8;
    val_ts_begin timestamptz; --included
    val_ts_end timestamptz; -- excluded
    val_ts_begin_saved timestamptz;
    val_ts_end_saved timestamptz;
    val_ts timestamptz;
    val_ts_prev timestamptz;
    val_shop_id int8;
	val_frequent_users roaringbitmap;
    val_total roaringbitmap;
BEGIN
    FOR val_shop_id IN
        SELECT shop_id FROM visit_stats_uv GROUP BY shop_id
    LOOP
	    FOR val_i IN 0..array_length(precisions, 1) LOOP 
	        -- binary search for the first empty frequent_users
	        SELECT min(visit_time) INTO val_ts_begin FROM visit_stats_uv WHERE shop_id=val_shop_id AND precision=precisions[val_i];
	        IF val_ts_begin IS NULL THEN
                CONTINUE;
            END IF;
            -- assumes #frequent_users!=0 OR #total!=0 for WHERE shop_id=val_shop_id AND precision=precisions[val_i] AND visit_time=val_ts_begin
            val_ts_begin = date_trunc(precisions[val_i], val_ts_begin + prec_intervals[val_i]);
            val_ts_end = date_trunc(precisions[val_i], timestamptz 'now' + prec_intervals[val_i]);
            val_ts_begin_saved = val_ts_begin;
            val_ts_end_saved = val_ts_end;
            WHILE val_ts_begin < val_ts_end LOOP 
                val_ts = date_trunc(precisions[val_i], val_ts_begin + (val_ts_end - val_ts_begin) / double precision '2.0');
                SELECT rb_cardinality(frequent_users) INTO val_cnt FROM visit_stats_uv WHERE shop_id=val_shop_id AND precision=precisions[val_i] AND visit_time=val_ts;
                IF val_cnt IS NULL OR val_cnt=0 THEN
                    val_ts_end = val_ts;
                ELSE 
                    val_ts_begin = date_trunc(precisions[val_i], val_ts + prec_intervals[val_i]);
                END IF;
            END LOOP;
            val_ts = val_ts_end;
            
            -- update frequent_users since val_ts
            IF val_ts>=val_ts_end_saved THEN
                CONTINUE;
            END IF;
            RAISE NOTICE 'updating frequent_users since WHERE shop_id=% AND precision=% AND visit_time=%', val_shop_id, precisions[val_i], val_ts;

  	        val_ts_prev = date_trunc(precisions[val_i], val_ts - prec_intervals[val_i]);
	        SELECT frequent_users, total INTO val_frequent_users, val_total FROM visit_stats_uv WHERE shop_id=val_shop_id AND precision=precisions[val_i] AND visit_time=val_ts_prev;
            IF val_total IS NULL THEN
                val_frequent_users = '{}'::roaringbitmap;
        	ELSE
                val_frequent_users = val_frequent_users | val_total;
        	END IF;
	        WHILE val_ts < val_ts_end_saved LOOP 
                SELECT total INTO val_total FROM visit_stats_uv WHERE shop_id=val_shop_id AND precision=precisions[val_i] AND visit_time=val_ts;
                IF val_total IS NULL THEN
                    INSERT INTO visit_stats_uv AS v(shop_id, precision, visit_time, frequent_users) VALUES (val_shop_id, precisions[val_i], val_ts, val_frequent_users); 
                ELSE
                    UPDATE visit_stats_uv SET frequent_users=val_frequent_users WHERE shop_id=val_shop_id AND precision=precisions[val_i] AND visit_time=val_ts; 
                    val_frequent_users = val_frequent_users | val_total;
                END IF;
	            val_ts = val_ts + prec_intervals[val_i];
	            val_ret = val_ret + 1;
            END LOOP;
	    END LOOP;
    END LOOP;
	RETURN val_ret;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION forget_old_visits() RETURNS int AS $$
DECLARE
    val_hour int8;
BEGIN
	val_hour = EXTRACT(EPOCH FROM timestamptz 'now' - interval '10 year')/(60*60);
	UPDATE visit_stats_user SET hours=rb_clear(hours, 0, val_hour);
	RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION random_between(low int, high int) RETURNS INT AS $$
BEGIN
   RETURN floor(random() * (high-low + 1) + low);
END;
$$ language 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION restore_visit_stats() RETURNS int AS $$
DECLARE
    val_shop_id int8;
    val_uid int8;
    val_visit_time timestamptz;
	val_gender int;
	val_age int;
BEGIN
    DELETE FROM visit_stats_user;
    DELETE FROM visit_stats_pv;
    DELETE FROM visit_stats_uv;
    FOR val_shop_id, val_uid, val_visit_time IN
        SELECT shop_id, uid, visit_time FROM visit_events WHERE position=0 ORDER BY visit_time
    LOOP
	    SELECT gender, age INTO val_gender, val_age FROM users WHERE uid=val_uid ORDER BY quality LIMIT 1;
	    IF val_gender IS NULL THEN
	        val_gender = random_between(0, 2);
	        val_age = random_between(15, 66);
   	    END IF;
   	    PERFORM insert_visit_stats(val_shop_id, val_uid, val_gender, val_age, val_visit_time);
    END LOOP;
    PERFORM update_frequent_users();
	RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_shuke_stats(param_shop_id int8, param_day_start timestamptz, param_day_end timestamptz) RETURNS table(vt timestamptz, first_users int, multi_users int) AS $$
BEGIN
    param_day_start = date_trunc('day', param_day_start);
    param_day_end = date_trunc('day', param_day_end);
    RETURN QUERY SELECT visit_time, rb_andnot_cardinality(total, frequent_users)::int AS first_users, rb_and_cardinality(total, frequent_users)::int AS multi_users FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start AND visit_time<param_day_end ORDER BY visit_time;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_shuke_dist(param_shop_id int8, param_day_start timestamptz, param_day_end timestamptz) RETURNS table(g0 int8, g1 int8, a0 int8, a1 int8, a2 int8, a3 int8, a4 int8, a5 int8, a6 int8, a7 int8, a8 int8, a9 int8, a10 int8, a11 int8, a12 int8, a13 int8, a14 int8, a15 int8, a16 int8, a17 int8, a18 int8, a19 int8) AS $$
BEGIN
    param_day_start = date_trunc('day', param_day_start);
    param_day_end = date_trunc('day', param_day_end);
    RETURN QUERY SELECT rb_cardinality(rb_or_agg(gender_0)) as g0, rb_cardinality(rb_or_agg(gender_1)) as g1, rb_cardinality(rb_or_agg(age_0)) as a2, rb_cardinality(rb_or_agg(age_1)) as a1, rb_cardinality(rb_or_agg(age_2)) as a2, rb_cardinality(rb_or_agg(age_3)) as a3, rb_cardinality(rb_or_agg(age_4)) as a4, rb_cardinality(rb_or_agg(age_5)) as a5, rb_cardinality(rb_or_agg(age_6)) as a6, rb_cardinality(rb_or_agg(age_7)) as a7, rb_cardinality(rb_or_agg(age_8)) as a8, rb_cardinality(rb_or_agg(age_9)) as a9, rb_cardinality(rb_or_agg(age_10)) as a10, rb_cardinality(rb_or_agg(age_11)) as a11, rb_cardinality(rb_or_agg(age_12)) as a12, rb_cardinality(rb_or_agg(age_13)) as a13, rb_cardinality(rb_or_agg(age_14)) as a14, rb_cardinality(rb_or_agg(age_15)) as a15, rb_cardinality(rb_or_agg(age_16)) as a16, rb_cardinality(rb_or_agg(age_17)) as a17, rb_cardinality(rb_or_agg(age_18)) as a18, rb_cardinality(rb_or_agg(age_19)) as a19 FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start AND visit_time<param_day_end;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_huitouke_stats(param_shop_id int8, param_day_start timestamptz, param_day_end timestamptz) RETURNS table(vt timestamptz, first_users int, multi_users int) AS $$
DECLARE
    val_frequent_users roaringbitmap;
BEGIN
    param_day_start = date_trunc('day', param_day_start);
    param_day_end = date_trunc('day', param_day_end);
    SELECT rb_or_agg(total) INTO val_frequent_users FROM (SELECT total FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start - interval '90 days' AND visit_time<param_day_start) foo;
    IF val_frequent_users IS NULL THEN
        val_frequent_users = roaringbitmap('{}');
    END IF;
    RETURN QUERY SELECT visit_time, rb_andnot_cardinality(total, val_frequent_users)::int AS first_users, rb_and_cardinality(total, val_frequent_users)::int AS multi_users FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start AND visit_time<param_day_end ORDER BY visit_time;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_huitouke_dist(param_shop_id int8, param_day_start timestamptz, param_day_end timestamptz) RETURNS table(gg0 int8, gg1 int8, aa0 int8, aa1 int8, aa2 int8, aa3 int8, aa4 int8, aa5 int8, aa6 int8, aa7 int8, aa8 int8, aa9 int8, aa10 int8, aa11 int8, aa12 int8, aa13 int8, aa14 int8, aa15 int8, aa16 int8, aa17 int8, aa18 int8, aa19 int8) AS $$
DECLARE
    val_frequent_users roaringbitmap;
BEGIN
    param_day_start = date_trunc('day', param_day_start);
    param_day_end = date_trunc('day', param_day_end);
    SELECT rb_or_agg(total) INTO val_frequent_users FROM (SELECT total FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start - interval '90 days' AND visit_time<param_day_start) foo;
    IF val_frequent_users IS NULL THEN
        val_frequent_users = roaringbitmap('{}');
    END IF;
    RETURN QUERY SELECT rb_cardinality(val_frequent_users&g0) as gg0, rb_cardinality(val_frequent_users&g1) as gg1, rb_cardinality(val_frequent_users&a0) as aa0, rb_cardinality(val_frequent_users&a1) as aa1, rb_cardinality(val_frequent_users&a2) as aa2, rb_cardinality(val_frequent_users&a3) as aa3, rb_cardinality(val_frequent_users&a4) as aa4, rb_cardinality(val_frequent_users&a5) as aa5, rb_cardinality(val_frequent_users&a6) as aa6, rb_cardinality(val_frequent_users&a7) as aa7, rb_cardinality(val_frequent_users&a8) as aa8, rb_cardinality(val_frequent_users&a9) as aa9, rb_cardinality(val_frequent_users&a10) as aa10, rb_cardinality(val_frequent_users&a11) as aa11, rb_cardinality(val_frequent_users&a12) as aa12, rb_cardinality(val_frequent_users&a13) as aa13, rb_cardinality(val_frequent_users&a14) as aa14, rb_cardinality(val_frequent_users&a15) as aa15, rb_cardinality(val_frequent_users&a16) as aa16, rb_cardinality(val_frequent_users&a17) as aa17, rb_cardinality(val_frequent_users&a18) as aa18, rb_cardinality(val_frequent_users&a19) as aa19 FROM (SELECT rb_or_agg(gender_0) as g0, rb_or_agg(gender_1) as g1, rb_or_agg(age_0) as a0, rb_or_agg(age_1) as a1, rb_or_agg(age_2) as a2, rb_or_agg(age_3) as a3, rb_or_agg(age_4) as a4, rb_or_agg(age_5) as a5, rb_or_agg(age_6) as a6, rb_or_agg(age_7) as a7, rb_or_agg(age_8) as a8, rb_or_agg(age_9) as a9, rb_or_agg(age_10) as a10, rb_or_agg(age_11) as a11, rb_or_agg(age_12) as a12, rb_or_agg(age_13) as a13, rb_or_agg(age_14) as a14, rb_or_agg(age_15) as a15, rb_or_agg(age_16) as a16, rb_or_agg(age_17) as a17, rb_or_agg(age_18) as a18, rb_or_agg(age_19) as a19 FROM (SELECT gender_0, gender_1, age_0, age_1, age_2, age_3, age_4, age_5, age_6, age_7, age_8, age_9, age_10, age_11, age_12, age_13, age_14, age_15, age_16, age_17, age_18, age_19 FROM visit_stats_uv WHERE shop_id=param_shop_id AND precision='day' AND visit_time>=param_day_start AND visit_time<param_day_end) foo) bar;
END;
$$ LANGUAGE plpgsql;


-- https://stackoverflow.com/questions/1348126/modify-owner-on-all-tables-simultaneously-in-postgresql
CREATE OR REPLACE FUNCTION alter_database_owner_to(param_new_owner text) RETURNS void AS $$
DECLARE
    val_db text;
    val_obj text;
    val_schemaname text;
    val_tablename text;
    val_sequence_schema text;
    val_sequence_name text;
    val_table_schema text;
    val_table_name text;
BEGIN
    SELECT current_database() INTO val_db;
    EXECUTE 'ALTER DATABASE ' || val_db || ' OWNER TO ' || param_new_owner;

    FOR val_schemaname, val_tablename IN
        SELECT schemaname, tablename FROM pg_tables WHERE NOT schemaname IN ('pg_catalog', 'information_schema')
    LOOP
        val_obj = val_schemaname || '.' || val_tablename;
        EXECUTE 'ALTER TABLE ' || val_obj || ' OWNER TO ' || param_new_owner;
    END LOOP;

    FOR val_sequence_schema, val_sequence_name IN
        SELECT sequence_schema, sequence_name FROM information_schema.sequences WHERE NOT sequence_schema IN ('pg_catalog', 'information_schema')
    LOOP
        val_obj = val_sequence_schema || '.' || val_sequence_name;
        EXECUTE 'ALTER SEQUENCE ' || val_obj || ' OWNER TO ' || param_new_owner;
    END LOOP;

    FOR val_table_schema, val_table_name IN
        SELECT table_schema, table_name FROM information_schema.views WHERE NOT table_schema IN ('pg_catalog', 'information_schema')
    LOOP
        val_obj = val_table_schema || '.' || val_table_name;
        EXECUTE 'ALTER SEQUENCE ' || val_obj || ' OWNER TO ' param_new_owner;
    END LOOP;

    FOR val_obj IN
        SELECT oid::regclass::text FROM pg_class WHERE relkind = 'm' ORDER BY oid
    LOOP
        EXECUTE 'ALTER TABLE ' || val_obj || ' OWNER TO ' || param_new_owner;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT alter_database_owner_to('mcd');
