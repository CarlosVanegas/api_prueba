import boto3
import pandas as pd
from decimal import Decimal
from botocore.exceptions import ClientError
from pandas.errors import EmptyDataError

def upload_data_to_dynamodb(table_name, csv_file_path):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        df = pd.read_csv(csv_file_path)

        if df.empty:
            return {"status": "error", "message": "El archivo CSV está vacío."}

        df.drop_duplicates(inplace=True)

        # Asegurar que unique_id sea verdaderamente único para cada fila, incluso entre cargas de datos
        # Aquí se asume que puedes generar un unique_id único de alguna manera
        # Si tus datos ya incluyen un identificador único, utiliza ese campo directamente
        df['unique_id'] = pd.util.hash_pandas_object(df, index=True).astype(str)

        # Verificar duplicados
        seen = set()
        unique_data = []
        for index, row in df.iterrows():
            if row['unique_id'] not in seen:
                seen.add(row['unique_id'])
                unique_data.append(row)
            else:
                print(f"Duplicado encontrado para unique_id: {row['unique_id']}")

        for item in unique_data:
            for key, value in item.items():
                if isinstance(value, float):
                    item[key] = Decimal(str(value))

            # Aquí se asume que unique_id es la clave principal (Partition Key)
            item_to_put = {'unique_id': item['unique_id']}
            for key, value in item.items():
                if key != 'unique_id':  # No incluir unique_id si ya se ha usado como clave principal
                    item_to_put[key] = value
            table.put_item(Item=item_to_put)

        return {"status": "success", "message": "Datos subidos correctamente a DynamoDB."}

    except EmptyDataError as e:
        return {"status": "error", "message": f"Error al leer el archivo CSV: {e}"}
    except ClientError as e:
        return {"status": "error", "message": f"Error al subir datos a DynamoDB: {e}"}
    except Exception as e:
        return {"status": "error", "message": f"Error desconocido: {e}"}
