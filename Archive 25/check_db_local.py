import sqlite3
import os

db_path = r"dea_local.db"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    # try instance folder anyway
    db_path = r"instance\ag_de_agent.db"
    if not os.path.exists(db_path):
        print(f"DB also not found at {db_path}")
        # check all .db files
        dbs = [f for f in os.listdir('.') if f.endswith('.db')]
        print(f"Found .db files: {dbs}")
        if dbs: db_path = dbs[0]
        else: exit(1)

print(f"Checking {db_path}")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT dataset_id, client_name, source_folder, source_object, is_active FROM master_config_authoritative WHERE client_name='ADLSV1'")
rows = cursor.fetchall()
print("DATASET_ID | CLIENT | FOLDER | OBJECT | ACTIVE")
if not rows:
    print("NO ROWS FOR ADLSV1")
    cursor.execute("SELECT DISTINCT client_name FROM master_config_authoritative")
    print(f"Clients in DB: {cursor.fetchall()}")
for r in rows:
    print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]}")
conn.close()
