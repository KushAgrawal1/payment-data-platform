import psycopg2

try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="payment_dw",
        user="postgres",
        password="postgres_secure_password"  # <-- Updated Password
    )
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()
    print("✅ CONNECTION SUCCESSFUL! Tables found in DB:")
    for table in tables:
        print(f" - {table[0]}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ DATABASE CONNECTION FAILED: {str(e)}")