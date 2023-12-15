-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION arrow" to load this file. \quit

CREATE FUNCTION arrowam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD arrow TYPE TABLE HANDLER arrowam_handler;
COMMENT ON ACCESS METHOD arrow IS 'In-memory columnar table access method based on Apache Arrow format';
