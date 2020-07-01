
CREATE SCHEMA IF NOT EXISTS ${TARGET_DB};

DROP TABLE IF EXISTS ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema;

CREATE TABLE ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema
(
    id                  VARCHAR(256),
    double_value        DOUBLE,
    timestamp_value     TIMESTAMP,
    int_value           INT,
    float_value         FLOAT,
    bigint_value        BIGINT
);

INSERT INTO ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema values ('1',100.0,NULL,10, 101.1, 1000000);




