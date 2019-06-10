--echo ########################################################################
--echo # Verify the IS_VERSION and the CREATE VIEW statements of the
--echo # I_S system view.
--echo ########################################################################

--source include/have_debug.inc
SET debug = '+d,skip_dd_table_access_check,fetch_system_view_definition';

--echo ########################################################################
--echo # The actual I_S version stored on disk.
--echo ########################################################################
let $str = `SELECT SUBSTRING_INDEX(SUBSTRING(properties,
              LOCATE('IS_VERSION', properties), 30), ';', 1)
              AS I_S_VERSION
              FROM mysql.dd_properties`;
--source include/write_line.inc

--echo ########################################################################
--echo # List the CREATE VIEW statements for the I_S system views.
--echo # Mask collations that depend on the lower case table names setting.
--echo ########################################################################

# The debug variable 'fetch_system_view_definition' enables TABLE_COMMENT
# column of INFORMATION_SCHEMA.TABLES to return the CREATE VIEW command
# definition that the server uses to create system view during bootstrap.
#
# Note that we cannot use system view definition stored in DD column
# mysql.tables.view_definition(_utf8), because the SELECT query
# representing the view is re-written by optimizer. This causes difference
# in view definition syntax from 8.0 vs trunk. And the this difference
# would increase as optimizer implementation re-write SELECT query. In
# order to avoid these diffierences, we read system view definition that is
# hardcoded in source.

let $SELECT_CMD = SELECT TABLE_COMMENT
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_SCHEMA='information_schema';
let $INSERT_CMD = INSERT INTO I_S_check_table(t) $SELECT_CMD;

let $WHERE_COND = AND TABLE_NAME='CHARACTER_SETS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='CHECK_CONSTRAINTS';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='COLLATIONS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='COLLATION_CHARACTER_SET_APPLICABILITY';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='COLUMNS';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='COLUMN_STATISTICS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='EVENTS';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='FILES';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='INNODB_DATAFILES';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='INNODB_FOREIGN';
replace_regex /(fk.name\)) COLLATE utf8_tolower_ci/\1/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='INNODB_FOREIGN_COLS';
replace_regex /(fk.name\)) COLLATE utf8_tolower_ci/\1/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='INNODB_FIELDS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='INNODB_TABLESPACES_BRIEF';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='KEY_COLUMN_USAGE';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='KEYWORDS';
replace_regex /JSON_TABLE\(.*\)/JSON_TABLE(<elements masked>)/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='PARAMETERS';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='PARTITIONS';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='REFERENTIAL_CONSTRAINTS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='RESOURCE_GROUPS';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='ROUTINES';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='SHOW_STATISTICS';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='SCHEMATA';
replace_regex /(cat|sch).name COLLATE utf8_tolower_ci/\1.name/
              /JSON_TABLE\(.*\)/JSON_TABLE(<elements masked>)/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='ST_SPATIAL_REFERENCE_SYSTEMS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='ST_UNITS_OF_MEASURE';
replace_regex /JSON_TABLE\(.*\)/JSON_TABLE(<elements masked>)/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='ST_GEOMETRY_COLUMNS';
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='STATISTICS';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='TABLE_CONSTRAINTS';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='TABLES';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='TRIGGERS';
replace_regex /(cat|sch|tbl).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='VIEW_ROUTINE_USAGE';
replace_regex /(cat|sch|vw).name COLLATE utf8_tolower_ci/\1.name/
              /vru.routine_(catalog|schema) COLLATE utf8_tolower_ci/vru.routine_\1/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='VIEW_TABLE_USAGE';
replace_regex /(cat|sch|vw).name COLLATE utf8_tolower_ci/\1.name/
              /vtu.table_(catalog|schema|name) COLLATE utf8_tolower_ci/vtu.table_\1/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

let $WHERE_COND = AND TABLE_NAME='VIEWS';
replace_regex /(cat|sch|vw).name COLLATE utf8_tolower_ci/\1.name/;
eval $SELECT_CMD $WHERE_COND;
eval $INSERT_CMD $WHERE_COND;

SET debug = '-d,skip_dd_table_access_check';
SET debug = '-d,fetch_system_view_definition';