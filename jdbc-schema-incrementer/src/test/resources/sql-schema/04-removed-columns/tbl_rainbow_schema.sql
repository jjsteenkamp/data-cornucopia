
DROP TABLE IF EXISTS ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema;

CREATE TABLE ${TARGET_DB}.${NAMESPACE}tbl_rainbow_schema
(
    id                  VARCHAR(256),
    timestamp_value     TIMESTAMP,
    int_value           INT
);





