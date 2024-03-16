import json

from flask import Flask
from scripts.sql_script import execute_sql_query

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/ejecutar-script')
def ejecutar_script():
    # Parámetros para ejecutar el script
    db_connection = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'database_clients_bam'
    }

    # Ruta del archivo de consulta SQL
    sql_file = 'querys/query_inciso_1.sql'

    # Nombre de la tabla destino
    destination_table = 'resultado_query_insiso_1'

    # Ruta del archivo de registro
    log_file = 'logs/logs_scripts.log'

    # Leer el contenido del archivo SQL
    with open(sql_file, 'r') as file:
        sql_query = file.read()

    # Ejecutar el script
    message = execute_sql_query(db_connection, sql_query, destination_table, log_file)

    return json.dumps({"message": message})

# Probando la función ejecutar_script
print(ejecutar_script())

if __name__ == '__main__':
    app.run()
