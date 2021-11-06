import time
import psycopg2
import re
import uuid
import session
import threading


def rewrite_query(query, context):
    plan_id = str(uuid.uuid4())
    query = query.strip()
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
