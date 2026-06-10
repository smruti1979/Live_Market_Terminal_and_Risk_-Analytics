import sqlite3

conn = sqlite3.connect('kafka_analytics.db')
cursor = conn.cursor()

print("\n--- LIVE USER PROFILES (AGGREGATED STATE) ---")
cursor.execute("SELECT * FROM user_profiles")
for row in cursor.fetchall():
    print(f"User {row[0]} | All-Time Total: ${row[1]:.2f} | 60s Total: ${row[2]:.2f} | 60s Avg: ${row[3]:.2f}")

print("\n--- RECENT RAW LEDGER (LAST 5 TRANSACTIONS) ---")
cursor.execute("SELECT * FROM transaction_ledger ORDER BY id DESC LIMIT 5")
for row in cursor.fetchall():
    print(f"ID: {row[0]} | User {row[1]} spent ${row[2]:.2f}")

conn.close()
