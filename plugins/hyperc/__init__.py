import time
import psycopg2
import re
import uuid
import session
import threading
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))
from collections import defaultdict
PID_TERMINATIONS = defaultdict(lambda: [0, time.time()])

def rewrite_query(query, context):
    plan_id = str(uuid.uuid4())
    query = query.strip()
    if query.upper().startswith("TRANSIT KILL"):
        pid = query.split()[-1].replace(";", "")
        try:
            int(pid)
        except:
            return f"SELECT 'ERROR Cannot kill pid {pid} - wrong pid number';";
        os.system(f"kill -KILL {pid}")
        return f"SELECT 'KILL PID {pid}';";
    if query.upper().startswith("TRANSIT CANCEL"):
        pid = query.split()[-1].replace(";", "")
        try:
            int(pid)
        except:
            return f"SELECT 'ERROR Cannot cancel {pid} - wrong pid number';";
        # TODO: check which cancel port this process is listening to!
        # STUB! just stop whoever is listening to port 8494
        import urllib.request
        for i in range(10):
            try:
                urllib.request.urlopen(f"http://localhost:{8494+i}/", timeout=0.5).read()
            except:
                pass
        return f"SELECT 'CANCEL PID {pid}';";
    if query.upper().startswith("TRAIN CANCEL"):
        pid = query.split()[-1].replace(";", "")
        try:
            int(pid)
        except:
            return f"SELECT 'ERROR Cannot cancel {pid} - wrong pid number';";
        # TODO: check which cancel port this process is listening to!
        # STUB! just stop whoever is listening to port 8494
        import urllib.request
        for i in range(10):
            try:
                urllib.request.urlopen(f"http://localhost:{9494+i}/", timeout=0.5).read()
            except:
                pass
        return f"SELECT 'CANCEL PID {pid}';";
    if "pg_terminate_backend(".upper() in query.upper():
        pid = query.split("(")[1].split(")")[0]
        PID_TERMINATIONS[pid][0] += 1
        if PID_TERMINATIONS[pid][0] > 1:
            if time.time() - PID_TERMINATIONS[pid][1] < 5: 
                del PID_TERMINATIONS[pid]
                os.system(f"kill -KILL {pid}")
            else:
                del PID_TERMINATIONS[pid]
        return query
    if query.upper().startswith("TRANSIT UPGRADE"):
        HYPERC_TRANSIT_FUNC = open(os.path.join(dir_path, "plpy_transit.py")).read()
        dbname = context['connect_params']['database']
        username = context['connect_params']['user']
        conn = psycopg2.connect(database=dbname, user="postgres", host="/tmp/")
        cur = conn.cursor()
        cur.execute('DROP PROCEDURE IF EXISTS hyperc_transit')
        cur.execute(f"""
CREATE PROCEDURE public.hyperc_transit(sql_command character varying, plan_id character varying)
    LANGUAGE plpython3u
AS $_$
{HYPERC_TRANSIT_FUNC}
$_$;""")
        return "SELECT 'LATEST TRANSIT FUNCTION INSTALLED';";
    if query.upper().startswith("TRANSIT INIT"):
        HYPERC_TRANSIT_FUNC = open(os.path.join(dir_path, "plpy_transit.py")).read()
        dbname = context['connect_params']['database']
        username = context['connect_params']['user']
        conn = psycopg2.connect(database=dbname, user="postgres", host="/tmp/")
        cur = conn.cursor()
        try:
            cur.execute('CREATE EXTENSION IF NOT EXISTS plpython3u')
            cur.execute('CREATE OR REPLACE TRUSTED LANGUAGE hyperc HANDLER plpython3_call_handler')
            cur.execute(f'ALTER PROCEDURAL LANGUAGE hyperc OWNER TO {username}')
            cur.execute(f"""
CREATE PROCEDURE public.hyperc_transit(sql_command character varying, plan_id character varying)
    LANGUAGE plpython3u
AS $_$
{HYPERC_TRANSIT_FUNC}
$_$;""")
            cur.execute(f'ALTER PROCEDURE public.hyperc_transit(sql_command character varying, plan_id character varying) OWNER TO {username}')
            cur.execute("""
CREATE TABLE IF NOT EXISTS public.hc_plan (
    step_num integer,
    proc_name character varying(512),
    summary text,
    data jsonb,
    id integer NOT NULL,
    plan_id character varying(100) NOT NULL,
    op_type character varying(20),
    created_time timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP
)""")
            cur.execute(f"ALTER TABLE public.hc_plan OWNER TO {username}")
            cur.execute(f"""
CREATE SEQUENCE public.hc_plan_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;""")
            cur.execute(f"ALTER TABLE public.hc_plan_id_seq OWNER TO {username}")
            cur.execute("ALTER SEQUENCE public.hc_plan_id_seq OWNED BY public.hc_plan.id")
            cur.execute("ALTER TABLE ONLY public.hc_plan ALTER COLUMN id SET DEFAULT nextval('public.hc_plan_id_seq'::regclass)")
            cur.execute("""
CREATE TABLE IF NOT EXISTS public.hc_log(
    id integer PRIMARY KEY NOT NULL,
    logline text,
    created_time timestamp(3) with time zone DEFAULT CURRENT_TIMESTAMP
)""")
            cur.execute(f"ALTER TABLE public.hc_log OWNER TO {username}")
            cur.execute(f"""
CREATE SEQUENCE public.hc_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;""")
            cur.execute(f"ALTER TABLE public.hc_log_id_seq OWNER TO {username}")
            cur.execute("ALTER SEQUENCE public.hc_log_id_seq OWNED BY public.hc_log.id")
            cur.execute("ALTER TABLE ONLY public.hc_log ALTER COLUMN id SET DEFAULT nextval('public.hc_log_id_seq'::regclass)")
            cur.execute("""
CREATE TABLE IF NOT EXISTS public.hc_settings
(
    parameter text PRIMARY KEY NOT NULL,
    type text NOT NULL,
    value text NOT NULL,
    description text NOT NULL
);""")
            cur.execute(f"ALTER TABLE IF EXISTS public.hc_settings OWNER to {username};")
            # cur.execute("""INSERT INTO "public"."hc_settings" ("parameter","type","value","description") VALUES ('DOWNWARD_TOTAL_PUSHES','str','5000000','Maximum problem size in state expansions to compute (prevents OOM by killing grounding task)')""")
            cur.execute("""INSERT INTO "public"."hc_settings" ("parameter","type","value","description") VALUES ('SOLVER_MAX_TIME','int','120','Maximum cutoff seconds to spend on search')""")
            cur.execute("""INSERT INTO "public"."hc_settings" ("parameter","type","value","description") VALUES ('USE_CACHE','str','1','Use compiler cache where possible')""")
            cur.close()
            conn.commit()
            conn.close()
        except Exception as e:
            if conn is not None:
                conn.close()
            return f"SELECT 'HYPERC INIT ERROR: %s';" % repr(e).replace('"', '').replace("'", '')
        return "SELECT 'HYPERC INIT DONE';";

    if query.upper().startswith("WAIT "):
        str_timeout = query.split()[1]
        timeout_s = float(str_timeout)
        if timeout_s < 0.3:
            timeout_s = 0.3
        proxy_conn = context["conn"]
        state_name = context["pg_conn"].name
        def return_n_exit():
            time.sleep(timeout_s)
            if state_name in session.PLAN_STATES:
                message = b'C\x00\x00\x00\tCALL\x00Z\x00\x00\x00\x05I'
                sent = proxy_conn.sock.send(message)
                proxy_conn.sent(sent)
                proxy_conn.sock.close()
                del session.PLAN_STATES[state_name]
        threading.Thread(target=return_n_exit).start()

        # query = query.replace("WAIT ", "", 1).replace("wait ", "", 1)
        query = query.split(str_timeout, 1)[1].strip()
    if query.startswith("TRANSIT ") or query.startswith("EXPLAIN TO ") or query.startswith("EXPLAIN TRANSIT ") or query.startswith("TRAIN "):
        query = query.replace("'", "''")
        session.PLAN_STATES[context["pg_conn"].name] = (plan_id, context["conn"], context["pg_conn"])
        return f"CALL hyperc_transit('{query}', '{plan_id}');"
    return query
