import mysql.connector
import logging
from datetime import datetime

def execute_sql_query(db_connection, sql_query, destination_table, log_file):
    # Configuración de logging
    logging.basicConfig(filename=log_file, level=logging.INFO)

    try:
        # Conexión a la base de datos origen
        connection = mysql.connector.connect(**db_connection)
        cursor = connection.cursor()

        # Ejecución de la consulta SQL
        cursor.execute(sql_query)

        # Guardar los resultados en la tabla destino
        for row in cursor.fetchall():
            # Convertir los valores decimales a cadenas de texto
            row = [str(value) if not isinstance(value, str) else f"'{value}'" for value in row]
            insert_query = f"INSERT INTO {destination_table} VALUES ({','.join(row)})"
            cursor.execute(insert_query)
            connection.commit()

            # Registro en el archivo de log
            logging.info(f"Inserción realizada: {insert_query}")

        # Registro en el archivo de log
        logging.info(f"Ejecución de la consulta {sql_query} completada correctamente a las {datetime.now()}")

        return "Datos insertados correctamente"

    except mysql.connector.Error as error:
        logging.error(f"Error al ejecutar la consulta: {error}")
        return f"Error al ejecutar la consulta: {error}"

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
