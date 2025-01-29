CREATE TABLE all_types_100000 (
    c_bool BOOLEAN,
    c_char CHAR(1),
    c_varchar VARCHAR(255),
    c_text TEXT,
    c_int INTEGER,
    c_bigint BIGINT,
    c_smallint SMALLINT,
    c_numeric NUMERIC(10,2),
    c_real REAL,
    c_double_precision DOUBLE PRECISION,
    c_timestamp TIMESTAMP WITHOUT TIME ZONE,
    c_timestamptz TIMESTAMP WITH TIME ZONE,
    c_date DATE,
    c_time TIME WITHOUT TIME ZONE,
    c_timetz TIME WITH TIME ZONE,
    c_interval INTERVAL,
    c_bytea BYTEA,
    c_json JSON,
    c_jsonb JSONB,
    c_uuid UUID
);

CREATE TABLE all_types_1 (
    c_bool BOOLEAN,
    c_char CHAR(1),
    c_varchar VARCHAR(255),
    c_text TEXT,
    c_int INTEGER,
    c_bigint BIGINT,
    c_smallint SMALLINT,
    c_numeric NUMERIC(10,2),
    c_real REAL,
    c_double_precision DOUBLE PRECISION,
    c_timestamp TIMESTAMP WITHOUT TIME ZONE,
    c_timestamptz TIMESTAMP WITH TIME ZONE,
    c_date DATE,
    c_time TIME WITHOUT TIME ZONE,
    c_timetz TIME WITH TIME ZONE,
    c_interval INTERVAL,
    c_bytea BYTEA,
    c_json JSON,
    c_jsonb JSONB,
    c_uuid UUID
);

-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

INSERT INTO all_types_1 (c_bool, c_char, c_varchar, c_text, c_int, c_bigint, c_smallint, c_numeric, c_real, c_double_precision, c_timestamp, c_timestamptz, c_date, c_time, c_timetz, c_interval, c_bytea, c_json, c_jsonb, c_uuid)
SELECT 
    (random() > 0.5)::BOOLEAN,
    chr(65 + (random() * 25)::integer),
    md5(random()::text),
    md5(random()::text),
    (random() * 1000000)::integer,
    (random() * 1000000000000)::bigint,
    (random() * 10000)::smallint,
    (random() * 1000)::numeric(10,2),
    random()::real,
    random()::double precision,
    now() + (random() * 10000) * interval '1 day',
    now() + (random() * 10000) * interval '1 day',
    now()::date + (random() * 10000) * interval '1 day',
    now()::time + (random() * 10000) * interval '1 second',
    now()::timetz + (random() * 10000) * interval '1 second',
    (random() * 10000) * interval '1 day',
    decode(md5(random()::text), 'hex'),
    '{"string": "example string \",$^''`[]{}()+-_=!?<>\\", "integer": 123, "float": 3.14159, "boolean": true, "null": null, "array": [1, 2, 3], "object": {"nested_key": "nested_value" }}',
    '{"string": "example string \",$^''`[]{}()+-_=!?<>\\", "integer": 123, "float": 3.14159, "boolean": false, "null": null, "array": [], "object": {"nested_key": "nested_value", "another_nested_key": "another_nested_value" }}',
    gen_random_uuid ()
FROM generate_series(1, 1);

INSERT INTO all_types_100000 (c_bool, c_char, c_varchar, c_text, c_int, c_bigint, c_smallint, c_numeric, c_real, c_double_precision, c_timestamp, c_timestamptz, c_date, c_time, c_timetz, c_interval, c_bytea, c_json, c_jsonb, c_uuid)
SELECT 
    (random() > 0.5)::BOOLEAN,
    chr(65 + (random() * 25)::integer),
    md5(random()::text),
    md5(random()::text),
    (random() * 1000000)::integer,
    (random() * 1000000000000)::bigint,
    (random() * 10000)::smallint,
    (random() * 1000)::numeric(10,2),
    random()::real,
    random()::double precision,
    now() + (random() * 10000) * interval '1 day',
    now() + (random() * 10000) * interval '1 day',
    now()::date + (random() * 10000) * interval '1 day',
    now()::time + (random() * 10000) * interval '1 second',
    now()::timetz + (random() * 10000) * interval '1 second',
    (random() * 10000) * interval '1 day',
    decode(md5(random()::text), 'hex'),
    (SELECT row_to_json(t) FROM (SELECT * FROM all_types_1) t),
    (SELECT row_to_json(t)::jsonb FROM (SELECT * FROM all_types_1) t),
    gen_random_uuid ()
FROM generate_series(1, 100000);

ANALYZE all_types_100000;
ANALYZE all_types_1;
