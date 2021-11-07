import time
import psycopg2
import re
import uuid
import session
import threading
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))

def rewrite_query(query, context):
    plan_id = str(uuid.uuid4())
    query = query.strip()
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
    if query.startswith("TRANSIT ") or query.startswith("EXPLAIN TO ") or query.startswith("EXPLAIN TRANSIT "):
        query = query.replace("'", "''")
        session.PLAN_STATES[context["pg_conn"].name] = (plan_id, context["conn"], context["pg_conn"])
        return f"CALL hyperc_transit('{query}', '{plan_id}');"
    return query
