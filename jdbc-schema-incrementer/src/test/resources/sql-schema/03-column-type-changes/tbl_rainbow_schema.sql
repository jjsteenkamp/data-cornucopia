
DROP TABLE IF EXISTS ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema;

CREATE TABLE ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema
(
    id                  VARCHAR(256),
    double_value        DOUBLE,
    timestamp_value     VARCHAR(256),
    int_value           VARCHAR(256),
    float_value         FLOAT,
    bigint_value        BIGINT
);




