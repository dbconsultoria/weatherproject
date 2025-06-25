import psycopg2

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'dbname': os.getenv('DB_NAME')
}

def export_metadata(filename="database_metadata.sql"):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with open(filename, "w", encoding="utf-8") as f:

        # === EXPORT CREATE TABLEs ===
        cursor.execute("""
            SELECT DISTINCT table_schema, table_name
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name;
        """)
        tables = cursor.fetchall()

        for schema, table in tables:
            f.write(f"-- =======================================\n")
            f.write(f"-- CREATE TABLE {schema}.{table}\n")
            f.write(f"-- =======================================\n")

            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table))
            columns = cursor.fetchall()

            f.write(f"CREATE TABLE {schema}.{table} (\n")
            col_lines = []
            for col_name, data_type, is_nullable, default in columns:
                line = f"    {col_name} {data_type}"
                if default:
                    line += f" DEFAULT {default}"
                if is_nullable == "NO":
                    line += " NOT NULL"
                col_lines.append(line)
            f.write(",\n".join(col_lines))

            # Primary key
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s;
            """, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
            if pk_cols:
                f.write(f",\n    PRIMARY KEY ({', '.join(pk_cols)})")

            # Foreign keys
            cursor.execute("""
                SELECT 
                    kcu.column_name,
                    ccu.table_schema AS foreign_schema,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s;
            """, (schema, table))
            fk_rows = cursor.fetchall()
            for col, ref_schema, ref_table, ref_col in fk_rows:
                f.write(f",\n    FOREIGN KEY ({col}) REFERENCES {ref_schema}.{ref_table}({ref_col})")

            f.write("\n);\n\n")

        # === EXPORT STORED PROCEDURES AND FUNCTIONS ===
        cursor.execute("""
            SELECT 
                n.nspname AS schema,
                p.proname AS name,
                pg_get_function_arguments(p.oid) AS arguments,
                pg_get_function_result(p.oid) AS return_type,
                l.lanname AS language,
                p.prokind AS kind,
                pg_get_functiondef(p.oid) AS definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_language l ON p.prolang = l.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema, name;
        """)
        procedures = cursor.fetchall()

        f.write("-- =======================================\n")
        f.write("-- STORED PROCEDURES AND FUNCTIONS\n")
        f.write("-- =======================================\n\n")

        for schema, name, args, return_type, lang, kind, definition in procedures:
            kind_str = {
                'f': 'FUNCTION',
                'p': 'PROCEDURE',
                'a': 'AGGREGATE',
                'w': 'WINDOW'
            }.get(kind, 'UNKNOWN')

            f.write(f"-- {schema}.{name} ({kind_str})\n")
            f.write(f"-- Language: {lang}, Returns: {return_type}\n")
            f.write(definition.strip() + "\n\n")

    cursor.close()
    conn.close()
    print(f"✅ Metadata (tables + procedures) exported to {filename}")

if __name__ == "__main__":
    export_metadata()
