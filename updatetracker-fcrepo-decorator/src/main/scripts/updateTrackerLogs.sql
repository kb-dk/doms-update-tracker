--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: updatetrackerlogs; Type: TABLE; Schema: public; Owner: domsFieldSearch; Tablespace: 
--

CREATE TABLE updatetrackerlogs (
    key bigint NOT NULL,
    pid character varying(64) NOT NULL,
    happened timestamp with time zone NOT NULL,
    method character varying(64) NOT NULL,
    param character varying(64)
);


ALTER TABLE public.updatetrackerlogs OWNER TO "domsFieldSearch";

--
-- Name: updatetrackerlogs_key_seq; Type: SEQUENCE; Schema: public; Owner: domsFieldSearch
--

CREATE SEQUENCE updatetrackerlogs_key_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.updatetrackerlogs_key_seq OWNER TO "domsFieldSearch";

--
-- Name: updatetrackerlogs_key_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: domsFieldSearch
--

ALTER SEQUENCE updatetrackerlogs_key_seq OWNED BY updatetrackerlogs.key;


--
-- Name: key; Type: DEFAULT; Schema: public; Owner: domsFieldSearch
--

ALTER TABLE ONLY updatetrackerlogs ALTER COLUMN key SET DEFAULT nextval('updatetrackerlogs_key_seq'::regclass);


--
-- Name: updatetrackerlogs_pkey; Type: CONSTRAINT; Schema: public; Owner: domsFieldSearch; Tablespace: 
--

ALTER TABLE ONLY updatetrackerlogs
    ADD CONSTRAINT updatetrackerlogs_pkey PRIMARY KEY (key);


--
-- Name: updatetrackerlogs_happened; Type: INDEX; Schema: public; Owner: domsFieldSearch; Tablespace: 
--

CREATE INDEX updatetrackerlogs_happened ON updatetrackerlogs USING btree (happened);


--
-- Name: updatetrackerlogs_pid; Type: INDEX; Schema: public; Owner: domsFieldSearch; Tablespace: 
--

CREATE INDEX updatetrackerlogs_pid ON updatetrackerlogs USING btree (pid);


--
-- PostgreSQL database dump complete
--

