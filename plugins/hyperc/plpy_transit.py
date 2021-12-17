# CREATE OR REPLACE PROCEDURE public.hyperc_transit(IN sql_command character varying)
#     LANGUAGE 'plpython3u'
# AS $BDY$

import logzero 
from logzero import logger 
import os
import importlib
# from typing import Any, TYPE_CHECKING

# if TYPE_CHECKING:
#     plpy: Any = {}
#     sql_command: Any = {}

import hyperc
import hyperc.settings
import json
from collections import defaultdict
import hyper_etable.etable
from itertools import combinations
logzero.logfile("/tmp/plhyperc.log")

import logging
plogger = logging.getLogger('hyperc_progress')
hinit = False
if len(plogger.handlers) < 1:
    hinit = True
    plogger.setLevel(logging.INFO)
    fh=logging.FileHandler('/tmp/hyperc.log')
    plogger.addHandler(fh)

from logging import StreamHandler
import psycopg2

class SelfHandler(StreamHandler):

    def __init__(self):
        StreamHandler.__init__(self)
        dbname = list(plpy.execute("SELECT current_database()"))[0]["current_database"]
        self.conn = psycopg2.connect(database=dbname, user="postgres", host="/tmp/")

    def emit(self, record):
        msg = self.format(record)
        cur = self.conn.cursor()
        cur.execute('INSERT INTO hc_log ("logline") VALUES (%s)', (msg,))
        self.conn.commit()

local_h = SelfHandler()
plogger.addHandler(local_h)
        
    


SQL_PROCEDURES = """
select n.nspname as function_schema,
    p.proname as function_name,
    p.prosrc as source,
    l.lanname as function_language,
    pg_get_function_arguments(p.oid) as function_arguments,
    t.typname as return_type
from pg_proc p
left join pg_namespace n on p.pronamespace = n.oid
left join pg_language l on p.prolang = l.oid
left join pg_type t on t.oid = p.prorettype 
where n.nspname not in ('pg_catalog', 'information_schema') and l.lanname = 'hyperc' and t.typname = 'void'
order by function_schema,
         function_name;
"""

SQL_GET_PRIMARYKEYS = """
SELECT c.column_name, c.data_type
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';
"""

SQL_GET_ALLCOLUMNS = """
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = '{table_name}';
"""

# load and apply settings
SETTINGS_TABLE_NAME = "hc_settings"
table_settings = plpy.execute(f"SELECT parameter, type, value from {SETTINGS_TABLE_NAME}")
for setting in table_settings:
    sname = str(setting["parameter"])
    stype = setting["type"]
    sval = setting["value"]
    if stype.lower() == "int":
        value = int(setting["value"])
    elif stype.lower() == "float":
        value = float(setting["value"])
    elif stype.lower() == "str":
        value = str(setting["value"])
    else:
        plpy.error(f"Unsupported setting type {stype} found in {SETTINGS_TABLE_NAME} table: {sname}({stype})={repr(sval)} - only 'str' and 'int' are supported")
    if not sname in hyperc.settings.__dict__:
        plpy.error(f"Unsupported setting name {sname} found in {SETTINGS_TABLE_NAME}")
    setattr(hyperc.settings, sname, value)
    os.environ[sname] = str(value)
    import hyperc.running
    importlib.reload(hyperc.running)


def to_sql(v):
    if type(v) == int:
        return repr(v)
    elif type(v) == float:
        plpy.error("Floating point numbers are not supported")
        return repr(v)
    elif v == None:
        plpy.error("NULL and None are not supported")
        return ""
    elif type(v) == str:
        return plpy.quote_literal(v)
    plpy.error(f"Unexpected type {type(v)} in value {repr(v)}")
    
    

logger.debug(sql_command)  

if not plan_id:
    import uuid
    local_plan_id = str(uuid.uuid4())
else:
    local_plan_id = plan_id
 
input_py = f'/tmp/actions_{local_plan_id}.py'
sql_command_l = sql_command
supported_commands = ["INSERT", "UPDATE"]

if not any(x in sql_command_l.upper() for x in supported_commands):
    raise NotImplementedError("Only INSERT and UPDATE are supported")

if "WITH" in sql_command_l:
    raise NotImplementedError("WITH statement not supported")


autotransit_commands = ["TRANSIT UPDATE", "TRANSIT INSERT", "TRANSIT TO"]

goal_func = ""
exec_sql = ""

all_executed_commands = []
tables_names = [] 

write_enabled = True
write_only_to = []

if any(x in sql_command_l.upper() for x in autotransit_commands) or sql_command_l.upper().startswith("EXPLAIN TO") or sql_command_l.upper().startswith("EXPLAIN TRANSIT"):
    if sql_command_l.upper().startswith("EXPLAIN ") and not sql_command_l.upper().startswith("EXPLAIN TO "):
        sql_command_l = sql_command_l.replace("EXPLAIN ", "", 1).replace("explain ", "", 1)
        write_enabled = False
    elif sql_command_l.upper().startswith("EXPLAIN TO "):
        sql_command_l = sql_command_l.replace("EXPLAIN TO ", "", 1).replace("explain to ", "", 1)
        if " update " in sql_command_l.lower() and not "'" in sql_command_l.lower().split(" update ")[0]:
            tables_to_explain_to, rest_of_command = sql_command_l.lower().split(" transit update ", 1)
        elif " insert " in sql_command_l.lower() and not "'" in sql_command_l.lower().split(" transit insert ")[0]:
            tables_to_explain_to, rest_of_command = sql_command_l.lower().split(" insert ", 1)

        old_sql_command_l = sql_command_l
        sql_command_l = sql_command_l.replace(tables_to_explain_to, "", 1)
        if sql_command_l == old_sql_command_l:
            plpy.error("Can not parse TRANSIT query. Table names uppercase not supported.")
        sql_command_l = sql_command_l.strip()
        write_only_to = [tn.strip() for tn in tables_to_explain_to.split(",")]

    txid1 = plpy.execute("SELECT txid_current();")[0]["txid_current"]
    txid2 = plpy.execute("SELECT txid_current();")[0]["txid_current"]
    if txid1 != txid2:
        plpy.error("Can not TRANSIT in autotransit mode inside an open transaction")
    
    if "TRANSIT TO" in sql_command_l.upper():
        goal_func = sql_command_l.lower().split("transit to")[1].strip().split()[0]
        table_names = [x.strip() for x in sql_command_l.lower().split(" from ")[1].split(",")]
    elif "TRANSIT UPDATE" in sql_command_l.upper():
        tables_names = [ sql_command_l.lower().split("transit update")[1].split()[0] ]
        exec_sql = sql_command_l.replace("TRANSIT UPDATE", "UPDATE", 1)
        exec_sql = exec_sql.replace("TRANSIT UPDATE".lower(), "UPDATE", 1)
    elif "TRANSIT INSERT INTO" in sql_command_l.upper():
        tables_names = [ sql_command_l.lower().split("transit insert into")[1].split()[0] ]
        exec_sql = sql_command_l.replace("TRANSIT INSERT INTO", "INSERT INTO", 1)
        exec_sql = exec_sql.replace("TRANSIT INSERT INTO".lower(), "INSERT INTO", 1)
    else:
        raise RuntimeError("Wrong TRANSIT parse"
)

    # plpy.execute("BEGIN;")
    # plpy.execute("SAVEPOINT hyperc_sp1;")
    if exec_sql:
        logger.debug(f"Executing {exec_sql}")
        plpy.execute(exec_sql)
        all_executed_commands.append(exec_sql)
else:
    plpy.error("TRANSIT to end a transaction is not yet implemented")

all_tables_names = [x["table_name"] for x in plpy.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'public';")]

write_only_to_tables = [] 
write_only_to_tables_cols = defaultdict(list)
for tblcol in write_only_to:
    if "." in tblcol:
        tablename, colname = tblcol.split(".")
        write_only_to_tables_cols[tablename].append(colname)
    else:
        tablename = tblcol
    write_only_to_tables.append(tablename)

EQ_CMP_OPS = [" == ", " != ", " < ", " > ", " <= ", " >= "]

input_parameters_classes = {}

sources_list = []
for src in plpy.execute(SQL_PROCEDURES):
    if not src['function_arguments'].strip(): continue  # ignore procedures without arguments
    args = ", ".join([f"{argpair.strip().split()[-2]}: {argpair.strip().split()[-1].upper()}_Class" for argpair in src['function_arguments'].split(",")])
    tables_names.extend([f"{argpair.strip().split()[-1]}" for argpair in src['function_arguments'].split(",")])
    input_parameters_classes[src['function_name']] = {f"{argpair.strip().split()[-2]}":f"{argpair.strip().split()[-1]}" for argpair in src['function_arguments'].split(",")}
    fun_src = f"""def {src['function_name']}({args}):\n    pass\n"""
    for src_line in src["source"].split("\n"):
        fun_src += "    "+src_line+"\n"
    if src['function_name'] == goal_func:
        fun_src += "    DATA.GOAL = True\n"
    compile(fun_src, f"PROCEDURE {src['function_name']}", 'exec')
    sources_list.append(fun_src)

stub_hashed_commands = " ".join(all_executed_commands)

if not goal_func:
    fun_argcount = defaultdict(lambda: 0)
    fun_args = []
    goal_fun_src = []
    # Now generate goal
    for tbl in all_tables_names:
        logger.debug(f"Scanning for change {tbl}")
        updates = plpy.execute(f"SELECT * FROM {tbl} WHERE xmin::text = ((txid_current()+1) % (2^32)::bigint)::text;")
        txid_current = plpy.execute(f"SELECT txid_current();")
        logger.debug(f"Seeing updates for {tbl}: {updates} with {txid_current}")
        local_varnames = []
        for updated_row in updates:
            logger.debug(f"Scanning for change {tbl} row {updated_row}")
            varname = f"selected_{tbl}_{fun_argcount[tbl]}"
            local_varnames.append(varname)
            fun_argcount[tbl] += 1
            fun_args.append(f"{varname}: {tbl.upper()}_Class")
            for k, v in dict(updated_row).items():
                if k.upper() not in stub_hashed_commands.upper():
                    continue
                logger.debug(f"Scanning for change {tbl} row {updated_row} item {k}, {v}")
                goal_fun_src.append(f"assert {varname}.{k.upper()} == {repr(v)}")
            for ineq_pair in combinations(local_varnames, 2):
                goal_fun_src.append(f"assert {ineq_pair[0]} != {ineq_pair[1]}")
    if len(goal_fun_src) == 0:
        goal_fun_src.append("pass")
    else:
        plpy.rollback()
    goal_fun_name = "goal_from_transaction_updates"
    s_fun_args = ", ".join(fun_args)
    goal_func_code = f"def {goal_fun_name}({s_fun_args}):\n    " + "\n    ".join(goal_fun_src)
    goal_func_code += "\n    DATA.GOAL = True\n"

    sources_list.append(goal_func_code)
    assert exec_sql, "Must have UPDATE or INSERT executed"


source = "\n".join(sources_list)
logger.debug(f"Actions:\n {source}")

with open(input_py, 'w') as file:
    file.write(source)

base = {}
for t_n in tables_names:
    base[t_n] = dict(enumerate(list(plpy.execute(f"SELECT * FROM {t_n}", 1000))))
    row_check = next(iter(base[t_n].values()))
    logger.debug(base[t_n])
    for col, v in row_check.items():
        if v is None:
            plpy.error(f"NULL and None values in tables for HyperC procedures are not supported (table `{t_n}` column `{col}`)")


et = hyper_etable.etable.ETable(project_name='test_connnection_trucks')
db_connector = et.open_from(path=base, has_header=True, proto='raw', addition_python_files=[input_py])
et.dump_py(out_filename='/tmp/classes.py')
et.solver_call_plan_n_exec()
os.remove(input_py)

updates = defaultdict(list)
inserts = defaultdict(list)

updates_q = defaultdict(list)
inserts_q = defaultdict(list)

updated_columns = defaultdict(list)

if write_enabled:
    tables = db_connector.get_update()
    logger.debug(f"Update: {tables}")
    for tablename, rows in tables.items():
        if len(rows) == 0:
            continue
        if write_only_to_tables and tablename not in write_only_to_tables: continue
        pks = {x["column_name"]:x["data_type"] for x in plpy.execute(SQL_GET_PRIMARYKEYS.format(table_name=tablename))}
        all_columns = {x["column_name"]:x["data_type"] for x in plpy.execute(SQL_GET_ALLCOLUMNS.format(table_name=tablename))}
        for _, row in rows.items():
            update_where_q = []
            update_where_kv = {}
            update_set_q = []
            update_set_kv = {}
            updates[tablename].append(row)
            for colname, val in row.items():
                if colname in pks:
                    update_where_q.append(f"{colname} = {repr(val)}")
                    update_where_kv[colname] = val
                else:
                    if type(val) == str and not "char" in all_columns[colname] and not "text" in all_columns[colname] and len(val) == 0:
                        logger.warning(f"Skipping update of unsupported type {all_columns[colname]} for {tablename}.{colname} with value {repr(val)}")
                        continue
                    if len(write_only_to_tables_cols[tablename]) > 0 and colname not in write_only_to_tables_cols[tablename]:
                        logger.debug(f"Skipping update of column {colname} as {tablename} has explicit column inclusions and {colname} is not included")
                        continue
                    updated_columns[tablename].append(colname)
                    update_set_q.append(f"{colname} = {repr(val)}")
                    update_set_kv[colname] = val
            if len(update_set_q) == 0: 
                logger.warning(f"Skipping empty update for {tablename}: {row}")
                continue  # should never happen!
            if len(update_where_q) == 0: 
                logger.warning(f"Skipping update for table without primary key {tablename}: {row}")
                continue 
            set_subq = ", ".join(update_set_q)
            where_subq = " AND ".join(update_where_q)
            query = f'UPDATE {tablename} SET {set_subq} WHERE {where_subq};'
            updates_q[tablename].append({
                "plan_id": local_plan_id,
                "step_num": -1,
                "proc_name": "",
                "op_type": "UPDATE",
                "summary": query,
                "data": json.dumps({"tablename": tablename, "values": update_set_kv, "where": update_where_kv})
            })
            logger.debug(f"Executing, {query}")
            plpy.execute(query)
        

    tables = db_connector.get_append()
    logger.debug(f"Append: {tables}")
    for tablename, rows in tables.items():
        if len(rows) == 0:
            continue
        if write_only_to_tables and tablename not in write_only_to_tables: continue
        for _, row in rows.items():
            inserts[tablename].append(row)
            val = ", ".join([f'\'{repr(val)}\'' for _, val in row.items()])
            col_name = ", ".join([f'"{col}"' for col, _ in row.items()])
            query = f"INSERT INTO {tablename} ({col_name}) VALUES ({val});"
            inserts_q[tablename].append({
                "plan_id": local_plan_id,
                "step_num": -1,
                "proc_name": "",
                "op_type": "INSERT",
                "summary": query,
                "data": json.dumps({"tablename": tablename, "values": dict(row)})
            })
            logger.debug(f"Executing, {query}")
            plpy.execute(query)

istep = 0

logger.debug(f"Plan: {et.metadata['store_simple']}")

for step in et.metadata["store_simple"]:
    func_object = step[0]
    if not func_object.__name__ in input_parameters_classes: continue
    func_kwargs_before = step[1]
    func_kwargs_after = step[2]
    step_data = {
        "proc_name": func_object.__name__,
        "parameters_classes": input_parameters_classes[func_object.__name__],
        "input_parameters": func_kwargs_before,
        "output_parameters": func_kwargs_after,
    }
    l_summary = []
    for k, values in func_kwargs_before.items():
        val_s = ",".join(["%s:%s" % (col,x) for col,x in values.items()])
        l_summary.append(f"{k}={val_s}")
    summary = "/".join(l_summary)
    query_ins = f"INSERT INTO hc_plan (plan_id, step_num, proc_name, summary, data, op_type) VALUES ('{local_plan_id}', {istep}, '{func_object.__name__}', '{summary}', '{json.dumps(step_data)}', 'STEP');"
    logger.debug(query_ins)
    plpy.execute(query_ins)
    istep += 1

logger.debug(f"UPS/INS {updates_q}, {inserts_q}")

for tablename, updates in updates_q.items():
    for up in updates:
        colq = ", ".join(list(up.keys()))
        valq = ", ".join([to_sql(x) for x in up.values()])
        query_ins = f"INSERT INTO hc_plan ({colq}) VALUES ({valq});"
        logger.debug(query_ins)
        plpy.execute(query_ins)


for tablename, updates in inserts_q.items():
    for up in updates:
        colq = ", ".join(list(up.keys()))
        valq = ", ".join([to_sql(x) for x in up.values()])
        query_ins = f"INSERT INTO hc_plan ({colq}) VALUES ({valq});"
        logger.debug(query_ins)
        plpy.execute(query_ins)

plogger.removeHandler(local_h)
local_h.conn.close()

# $BDY$;
