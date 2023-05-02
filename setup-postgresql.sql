CREATE DATABASE metrics_ht;
\c metrics_ht;

CREATE  TABLE "public".field_names (
   field_id             serial  NOT NULL  ,
   field_name           varchar(100)    ,
   description          varchar    ,
   CONSTRAINT pk_field_names PRIMARY KEY ( field_id ),
   CONSTRAINT unq_field_names UNIQUE ( field_name )
 );

CREATE  TABLE "public".metric_values (
   "timestamp"          timestamp  NOT NULL  ,
   field_id             integer    ,
   hostname             varchar(100)    ,
   device               varchar  NOT NULL  ,
   "value"              numeric(40,6)
 );

CREATE INDEX idx_metric_values ON "public".metric_values ( "timestamp", field_id );

CREATE USER metrics_user WITH PASSWORD 'notasecurepassword';

GRANT ALL PRIVILEGES ON DATABASE "metrics_ht" to metrics_user;

GRANT ALL ON field_names TO metrics_user;

GRANT ALL ON metric_values TO metrics_user;

GRANT USAGE, SELECT ON SEQUENCE field_names_field_id_seq TO metrics_user;
