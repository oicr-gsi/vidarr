--
-- PostgreSQL database dump
--

-- Dumped from database version 12.5 (Ubuntu 12.5-0ubuntu0.20.04.1)
-- Dumped by pg_dump version 12.5 (Ubuntu 12.5-0ubuntu0.20.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: update_modified(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_modified() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
	NEW.modified = now()::timestamptz(0);
	RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: active_operation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.active_operation (
    id integer NOT NULL,
    workflow_run_id integer NOT NULL,
    recovery_state jsonb NOT NULL,
    status character varying(20) NOT NULL,
    type character varying(255) NOT NULL
);


--
-- Name: active_operation_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.active_operation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: active_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.active_operation_id_seq OWNED BY public.active_operation.id;


--
-- Name: active_operation_workflow_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.active_operation_workflow_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: active_operation_workflow_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.active_operation_workflow_run_id_seq OWNED BY public.active_operation.workflow_run_id;


--
-- Name: active_workflow_run_extra_input_ids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.active_workflow_run_extra_input_ids (
    active_workflow_run_id integer NOT NULL,
    external_id_id integer NOT NULL
);


--
-- Name: active_workflow_extra_input_ids_active_workflow_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.active_workflow_extra_input_ids_active_workflow_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: active_workflow_extra_input_ids_active_workflow_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.active_workflow_extra_input_ids_active_workflow_run_id_seq OWNED BY public.active_workflow_run_extra_input_ids.active_workflow_run_id;


--
-- Name: active_workflow_extra_input_ids_external_id_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.active_workflow_extra_input_ids_external_id_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: active_workflow_extra_input_ids_external_id_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.active_workflow_extra_input_ids_external_id_id_seq OWNED BY public.active_workflow_run_extra_input_ids.external_id_id;


--
-- Name: active_workflow_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.active_workflow_run (
    id integer NOT NULL,
    external_input_ids_handled boolean NOT NULL,
    preflight_okay boolean NOT NULL,
    real_input jsonb,
    workflow_run_url character varying,
    cleanup_state jsonb,
    engine_phase integer,
    started timestamp with time zone,
    completed timestamp with time zone,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    target character varying(255)
);


--
-- Name: active_workflow_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.active_workflow_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: active_workflow_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.active_workflow_run_id_seq OWNED BY public.active_workflow_run.id;


--
-- Name: analysis; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.analysis (
    id integer NOT NULL,
    workflow_run_id integer NOT NULL,
    hash_id character varying NOT NULL,
    analysis_type character varying NOT NULL,
    file_path character varying,
    file_size bigint,
    file_md5sum character varying,
    file_metatype character varying,
    labels jsonb,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


--
-- Name: analysis_external_id; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.analysis_external_id (
    external_id_id integer NOT NULL,
    analysis_id integer NOT NULL
);


--
-- Name: analysis_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.analysis_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: analysis_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.analysis_id_seq OWNED BY public.analysis.id;


--
-- Name: external_id; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.external_id (
    id integer NOT NULL,
    workflow_run_id integer NOT NULL,
    external_id character varying NOT NULL,
    provider character varying NOT NULL,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    requested boolean DEFAULT false NOT NULL
);


--
-- Name: external_id_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.external_id_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: external_id_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.external_id_id_seq OWNED BY public.external_id.id;


--
-- Name: external_id_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.external_id_version (
    id integer NOT NULL,
    external_id_id integer NOT NULL,
    key character varying NOT NULL,
    value character varying NOT NULL,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    requested timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL
);


--
-- Name: external_id_version_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.external_id_version_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: external_id_version_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.external_id_version_id_seq OWNED BY public.external_id_version.id;


--
-- Name: workflow; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow (
    id integer NOT NULL,
    name character varying NOT NULL,
    is_active boolean NOT NULL,
    labels jsonb,
    max_in_flight integer,
    consumable_resources jsonb,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


--
-- Name: workflow_definition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_definition (
    id integer NOT NULL,
    workflow_language character varying,
    workflow_file character varying,
    hash_id character varying,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


--
-- Name: workflow_definition_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflow_definition_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflow_definition_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflow_definition_id_seq OWNED BY public.workflow_definition.id;


--
-- Name: workflow_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflow_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflow_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflow_id_seq OWNED BY public.workflow.id;


--
-- Name: workflow_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_run (
    id integer NOT NULL,
    workflow_version_id integer NOT NULL,
    hash_id character varying NOT NULL,
    labels jsonb,
    input_file_ids character varying[],
    arguments jsonb,
    metadata jsonb,
    last_accessed timestamp with time zone,
    started timestamp with time zone,
    completed timestamp with time zone,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    engine_parameters jsonb
);


--
-- Name: workflow_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflow_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflow_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflow_run_id_seq OWNED BY public.workflow_run.id;


--
-- Name: workflow_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_version (
    id integer NOT NULL,
    name character varying NOT NULL,
    version character varying NOT NULL,
    hash_id character varying NOT NULL,
    workflow_definition integer,
    parameters jsonb,
    metadata jsonb,
    created timestamp with time zone DEFAULT (now())::timestamp(0) with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


--
-- Name: workflow_version_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflow_version_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflow_version_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflow_version_id_seq OWNED BY public.workflow_version.id;


--
-- Name: active_operation id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_operation ALTER COLUMN id SET DEFAULT nextval('public.active_operation_id_seq'::regclass);


--
-- Name: active_operation workflow_run_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_operation ALTER COLUMN workflow_run_id SET DEFAULT nextval('public.active_operation_workflow_run_id_seq'::regclass);


--
-- Name: active_workflow_run_extra_input_ids active_workflow_run_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run_extra_input_ids ALTER COLUMN active_workflow_run_id SET DEFAULT nextval('public.active_workflow_extra_input_ids_active_workflow_run_id_seq'::regclass);


--
-- Name: active_workflow_run_extra_input_ids external_id_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run_extra_input_ids ALTER COLUMN external_id_id SET DEFAULT nextval('public.active_workflow_extra_input_ids_external_id_id_seq'::regclass);


--
-- Name: analysis id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis ALTER COLUMN id SET DEFAULT nextval('public.analysis_id_seq'::regclass);


--
-- Name: external_id id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id ALTER COLUMN id SET DEFAULT nextval('public.external_id_id_seq'::regclass);


--
-- Name: external_id_version id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id_version ALTER COLUMN id SET DEFAULT nextval('public.external_id_version_id_seq'::regclass);


--
-- Name: workflow id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow ALTER COLUMN id SET DEFAULT nextval('public.workflow_id_seq'::regclass);


--
-- Name: workflow_definition id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_definition ALTER COLUMN id SET DEFAULT nextval('public.workflow_definition_id_seq'::regclass);


--
-- Name: workflow_run id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run ALTER COLUMN id SET DEFAULT nextval('public.workflow_run_id_seq'::regclass);


--
-- Name: workflow_version id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_version ALTER COLUMN id SET DEFAULT nextval('public.workflow_version_id_seq'::regclass);


--
-- Name: active_operation active_operation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_operation
    ADD CONSTRAINT active_operation_pkey PRIMARY KEY (id);


--
-- Name: active_workflow_run_extra_input_ids active_workflow_extra_input_ids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run_extra_input_ids
    ADD CONSTRAINT active_workflow_extra_input_ids_pkey PRIMARY KEY (active_workflow_run_id, external_id_id);


--
-- Name: active_workflow_run active_workflow_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run
    ADD CONSTRAINT active_workflow_run_pkey PRIMARY KEY (id);


--
-- Name: analysis_external_id analysis_external_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis_external_id
    ADD CONSTRAINT analysis_external_id_pkey PRIMARY KEY (external_id_id, analysis_id);


--
-- Name: analysis analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis
    ADD CONSTRAINT analysis_pkey PRIMARY KEY (id);


--
-- Name: external_id external_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id
    ADD CONSTRAINT external_id_pkey PRIMARY KEY (id);


--
-- Name: external_id_version external_id_version_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id_version
    ADD CONSTRAINT external_id_version_pkey PRIMARY KEY (id);


--
-- Name: workflow workflow_name_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow
    ADD CONSTRAINT workflow_name_unique UNIQUE (name);


--
-- Name: workflow workflow_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow
    ADD CONSTRAINT workflow_pkey PRIMARY KEY (id);


--
-- Name: workflow_run workflow_run_hash_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run
    ADD CONSTRAINT workflow_run_hash_id_key UNIQUE (hash_id);


--
-- Name: workflow_run workflow_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run
    ADD CONSTRAINT workflow_run_pkey PRIMARY KEY (id);


--
-- Name: workflow_version workflow_version_hash_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_version
    ADD CONSTRAINT workflow_version_hash_id_key UNIQUE (hash_id);


--
-- Name: workflow_version workflow_version_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_version
    ADD CONSTRAINT workflow_version_pkey PRIMARY KEY (id);


--
-- Name: active_workflow_run active_workflow_run_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER active_workflow_run_update BEFORE INSERT OR UPDATE ON public.active_workflow_run FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: analysis analysis_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER analysis_update BEFORE INSERT OR UPDATE ON public.analysis FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: external_id external_id_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER external_id_update BEFORE INSERT OR UPDATE ON public.external_id FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: workflow_definition workflow_definition_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_definition_update BEFORE INSERT OR UPDATE ON public.workflow_definition FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: workflow_run workflow_run_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_run_update BEFORE INSERT OR UPDATE ON public.workflow_run FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: workflow workflow_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_update BEFORE INSERT OR UPDATE ON public.workflow FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: workflow_version workflow_version_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER workflow_version_update BEFORE INSERT OR UPDATE ON public.workflow_version FOR EACH ROW EXECUTE FUNCTION public.update_modified();


--
-- Name: active_workflow_run_extra_input_ids active_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run_extra_input_ids
    ADD CONSTRAINT active_workflow_run_id_fkey FOREIGN KEY (active_workflow_run_id) REFERENCES public.active_workflow_run(id);


--
-- Name: analysis_external_id analysis_external_id_analysis_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis_external_id
    ADD CONSTRAINT analysis_external_id_analysis_id_fkey FOREIGN KEY (analysis_id) REFERENCES public.analysis(id);


--
-- Name: analysis_external_id analysis_external_id_external_id_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis_external_id
    ADD CONSTRAINT analysis_external_id_external_id_id_fkey FOREIGN KEY (external_id_id) REFERENCES public.external_id(id);


--
-- Name: analysis analysis_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.analysis
    ADD CONSTRAINT analysis_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_run(id);


--
-- Name: active_workflow_run_extra_input_ids external_id_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_workflow_run_extra_input_ids
    ADD CONSTRAINT external_id_id_fkey FOREIGN KEY (external_id_id) REFERENCES public.external_id(id);


--
-- Name: external_id_version external_id_version_external_id_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id_version
    ADD CONSTRAINT external_id_version_external_id_id_fkey FOREIGN KEY (external_id_id) REFERENCES public.external_id(id) NOT VALID;


--
-- Name: external_id external_id_workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.external_id
    ADD CONSTRAINT external_id_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_run(id) NOT VALID;


--
-- Name: active_operation workflow_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_operation
    ADD CONSTRAINT workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_run(id);


--
-- Name: workflow_run workflow_run_workflow_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_run
    ADD CONSTRAINT workflow_run_workflow_version_id_fkey FOREIGN KEY (workflow_version_id) REFERENCES public.workflow_version(id) NOT VALID;


--
-- PostgreSQL database dump complete
--
