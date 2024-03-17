import json

from flask import Flask, jsonify, request
from scripts.sql_script import execute_sql_query
from scripts.script_enviar_datos_nube import upload_data_to_dynamodb
import mysql.connector

app = Flask(__name__)

# Configuración de la base de datos
db_connection = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'database_clients_bam'
}

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/ejecutar-script')
def ejecutar_script():
    # Parámetros para ejecutar el script

    # Ruta del archivo de consulta SQL
    sql_file = 'querys/query_inciso_1.sql'

    # Nombre de la tabla destino
    destination_table = 'test_query_inciso_1'

    # Ruta del archivo de registro
    log_file = 'logs/logs_scripts.log'

    # Leer el contenido del archivo SQL
    with open(sql_file, 'r') as file:
        sql_query = file.read()

    # Ejecutar el script
    message = execute_sql_query(db_connection, sql_query, destination_table, log_file)

    return json.dumps({"message": message})

# Probando la función ejecutar_script
# print(ejecutar_script())

@app.route('/subir')
def subir_data():
    # Nombre de la tabla DynamoDB donde se subirán los datos
    table_name = "datos_base_clientes"

    # Ruta al archivo CSV
    csv_file_path = "C:\\Users\\cvanegas\\Desktop\\Datos Gyt\\Prueba tecnica BAM\\Prueba\\datos_base_clientes.csv"

    # Subir datos a DynamoDB
    result = upload_data_to_dynamodb(table_name, csv_file_path)

    if result["status"] == "success":
        return result["message"]
    else:
        # Aquí puedes elegir cómo manejar los errores, por ejemplo:
        return f"Error al subir datos: {result['message']}", 400


@app.route('/customer', methods=['GET'])
def get_customer():
    customer_id = request.args.get('customerId')
    if not customer_id:
        return jsonify({'error': 'No se proporcionó el customerId'}), 400

    connection = mysql.connector.connect(**db_connection)
    try:
        cursor = connection.cursor(dictionary=True)
        # Modifica esta consulta para hacer JOIN con otras tablas según lo necesites
        sql = """
                SELECT 
                    c.CustomerID, c.AccountNumber, c.StoreID, c.TerritoryID, 
                    p.FirstName, p.MiddleName, p.LastName, p.EmailPromotion, p.Title, 
                    p.BusinessEntityID, p.PersonType, p.NameStyle, p.Suffix, 
                    p.AdditionalContactInfo, p.Demographics
                FROM database_clients_bam.customer AS c
                JOIN database_clients_bam.person AS p ON c.PersonID = p.BusinessEntityID
                WHERE c.CustomerID = %s
                """
        cursor.execute(sql, (customer_id,))
        result = cursor.fetchone()
        if result:
            # Aquí podrías realizar más consultas para enriquecer `result` con datos de otras tablas
            return jsonify(result)
        else:
            return jsonify({'error': 'Cliente no encontrado'}), 404
    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    app.run()
