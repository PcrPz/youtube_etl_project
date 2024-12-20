import pandas as pd
import psycopg2
from psycopg2 import sql
import os

db_config = {
    "user": "user",      
    "password": "password", 
    "host": "localhost",        
    "port": "5432",               
    "database": "youtube_data_db"    
}

connection_string = f"dbname={db_config['database']} user={db_config['user']} password={db_config['password']} host={db_config['host']} port={db_config['port']}"

def get_table_metadata(df):
    def map_dtypes(x):
        if (x == 'object') or (x == 'category') or (x == 'bool'):
            return 'TEXT'
        elif 'date' in x:
            return 'DATE'
        elif 'int' in x:
            return 'BIGINT'
        elif 'float' in x:
            return 'FLOAT'
    pg_dtypes = [map_dtypes(str(s)) for s in df.dtypes]
    table_metadata = ", ".join([f"{y} {x}" for x, y in zip(pg_dtypes, df.columns)])
    return table_metadata

def df_to_postgresql_table(table_name, operation, df, conn):
    cursor = conn.cursor()
    if operation == 'create_replace':
        df.columns = [c.lower() for c in df.columns]
        table_metadata = get_table_metadata(df)
        
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))
        cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table_name), sql.SQL(table_metadata)))
        
        if table_name in ['eu_dim_table', 'na_dim_table', 'as_dim_table']:
            if df['video_key'].duplicated().any():
                print(f"Warning: Duplicate video_key values found in {table_name}.")
            else:
                cursor.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (video_key)").format(sql.Identifier(table_name)))

        elif table_name == 'fact_table':
            cursor.execute("""
                ALTER TABLE fact_table
                ADD CONSTRAINT fk_eu FOREIGN KEY (eu_key) REFERENCES eu_dim_table (video_key),
                ADD CONSTRAINT fk_na FOREIGN KEY (na_key) REFERENCES na_dim_table (video_key),
                ADD CONSTRAINT fk_as FOREIGN KEY (as_key) REFERENCES as_dim_table (video_key);
            """)

        # Insert data
        for index, row in df.iterrows():
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                sql.SQL(', ').join(map(sql.Placeholder, df.columns))
            )
            cursor.execute(insert_query, row.to_dict())

        conn.commit()
        cursor.close()

    elif operation == 'insert':
        for index, row in df.iterrows():
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                sql.SQL(', ').join(map(sql.Placeholder, df.columns))
            )
            cursor.execute(insert_query, row.to_dict())
        conn.commit()
        cursor.close()

def sending_data_to_postgresql():
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir,"data")
    output_dir = os.path.join(data_dir,"zone")

    NA_Data = pd.read_csv(os.path.join(output_dir, "Clean_NA_data.csv"), low_memory=False, encoding='utf-8')
    AS_Data = pd.read_csv(os.path.join(output_dir, "Clean_AS_data.csv"), low_memory=False, encoding='utf-8')
    EU_Data = pd.read_csv(os.path.join(output_dir, 'Clean_EU_data.csv'), low_memory=False, encoding='utf-8')
    Fact_Table = pd.read_csv(os.path.join(output_dir, 'fact_table.csv'), low_memory=False)

    tables = [NA_Data, AS_Data, EU_Data, Fact_Table]
    tables_name = ['na_dim_table', 'as_dim_table', 'eu_dim_table', 'fact_table']

    conn = psycopg2.connect(connection_string)

    for df, title in zip(tables, tables_name):
        df_to_postgresql_table(title, 'create_replace', df, conn)

    conn.close()
