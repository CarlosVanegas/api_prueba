from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import boto3
import pandas as pd

def upload_batch_to_dynamodb(table, data):
    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)

def upload_data_to_dynamodb(table_name, csv_file_path, batch_size=100):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        df = pd.read_csv(csv_file_path)

        if df.empty:
            return {"status": "error", "message": "El archivo CSV está vacío."}

        # Asegurar que unique_id sea verdaderamente único para cada fila
        df['unique_id'] = pd.util.hash_pandas_object(df, index=True).astype(str)

        # Dividir el DataFrame en lotes más pequeños para procesamiento en paralelo
        batches = [df[i:i+batch_size] for i in range(0, df.shape[0], batch_size)]

        with ThreadPoolExecutor() as executor:
            for batch_df in batches:
                batch_data = []
                for _, row in batch_df.iterrows():
                    item = {'unique_id': row['unique_id']}
                    for key, value in row.items():
                        if isinstance(value, float):
                            value = Decimal(str(value))
                        item[key] = value
                    batch_data.append(item)
                executor.submit(upload_batch_to_dynamodb, table, batch_data)

        return {"status": "success", "message": "Datos subidos correctamente a DynamoDB."}

    except Exception as e:
        return {"status": "error", "message": f"Error desconocido: {e}"}
