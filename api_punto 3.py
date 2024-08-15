from flask import Flask, request, jsonify
import pandas as pd
import re
from collections import Counter

app = Flask(__name__)

# Código de procesamiento de datos proporcionado
df = pd.read_csv('C:\\Users\\ANDRES\\Downloads\\meli\\Experiments DataSet For Excercise-small (2).csv')
df['item_id'] = df['item_id'].fillna(0)
df = df[df['event_name'] != 'SEARCH']
df = df.sort_values(['user_id', 'timestamp', 'item_id'], ascending=[False, False, False])

df['is_duplicate'] = (
    (df['event_name'] == df['event_name'].shift(-1)) &
    (df['item_id'] == df['item_id'].shift(-1)) &
    (df['user_id'] == df['user_id'].shift(-1)) &
    (df['event_name'] != 'BUY')
)

indices_to_remove = df[df['is_duplicate']].index + 1
df = df[~df.index.isin(indices_to_remove)]
df = df.drop(columns='is_duplicate')

mapeo = {
    'SEARCH': 0,
    'PRODUCT': 1,
    'CHECKOUT_1': 2,
    'CHECKOUT_2': 3,
    'CHECKOUT_3': 4,
    'BUY': 5
}

df['event_num'] = df['event_name'].map(mapeo)
df = df.reset_index(drop=True)

def eliminar_filas(df):
    indices_a_eliminar = []
    for i in range(len(df) - 1):
        if df.loc[i + 1, 'event_num'] >= df.loc[i, 'event_num'] and df.loc[i + 1, 'event_num'] != 5:
            indices_a_eliminar.append(i + 1)

    if indices_a_eliminar:
        df = df.drop(indices_a_eliminar).reset_index(drop=True)
        df = eliminar_filas(df)  # Llamada recursiva
    return df

df_filtered = eliminar_filas(df)

df_buy = df[df['event_name'] == 'BUY']
df_buy['timestamp'] = pd.to_datetime(df_buy['timestamp'])
df_filtered['date'] = pd.to_datetime(df_filtered['timestamp']).dt.date
df_buy['date'] = pd.to_datetime(df_buy['timestamp']).dt.date

# Implementación del API Flask
@app.route('/experiment/<int:exp_id>/result', methods=['GET'])
def get_experiment_result(exp_id):
    # Obtener el parámetro de la fecha
    day = request.args.get('day')
    print('esta es la fecha: ',day)
    print('tipo: ',pd.to_datetime(day, format='%Y-%m-%d %H'))
    
    if not day:
        return jsonify({"error": "El parámetro 'day' es obligatorio"}), 400

    # Filtrar df_buy por la fecha especificada
    try:
        day_timestamp = pd.to_datetime(day, format='%Y-%m-%d %H')
        df_buy_filtered = df_buy[df_buy['timestamp'].dt.strftime('%Y-%m-%d %H') == day_timestamp.strftime('%Y-%m-%d %H')]
    except Exception as e:
        return jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD HH"}), 400

    # Si no hay datos en df_buy_filtered para ese día, retornar un error
    if df_buy_filtered.empty:
        return jsonify({"error": "No hay datos para el día especificado"}), 404

    # Realizar merge de df_filtered con df_buy_filtered
    df_merged = pd.merge(df_filtered, df_buy_filtered[['item_id', 'user_id', 'date']], on=['item_id', 'user_id', 'date'], how='inner')
    if df_merged.empty:
        return jsonify({"error": "No hay datos combinados después del merge"}), 404

    # Agrupar y concatenar los experimentos
    def concatenate_experiments(group):
        return ''.join(group['event_num'].astype(str))

    df_sorted = df_merged.sort_values(by=['user_id', 'item_id', 'event_num', 'timestamp'])
    df_filtered_final = df_sorted.drop_duplicates(subset=['user_id', 'item_id', 'event_num'], keep='first')

    df_grouped = df_filtered_final.groupby(['user_id', 'item_id']).apply(concatenate_experiments).reset_index(name='concatenated_experiments')
    df_grouped['count'] = df_grouped.groupby('concatenated_experiments')['concatenated_experiments'].transform('count')
    df_grouped['experiment_id'] = range(1, len(df_grouped) + 1)

    # Función para contar variantes y multiplicar por 'count'
    def count_variants(row):
        experiments = re.findall(r'\d+', row['concatenated_experiments'])
        variant_count = Counter(experiments)
        multiplied_count = {variant: count * row['count'] for variant, count in variant_count.items()}
        return multiplied_count

    # Aplicar la función a cada fila para obtener el conteo de variantes
    df_grouped['variant_counts'] = df_grouped.apply(count_variants, axis=1)

    # Filtrar por el ID del experimento
    df_experiment = df_grouped[df_grouped['experiment_id'] == exp_id]

    if df_experiment.empty:
        return jsonify({"error": "Experimento no encontrado"}), 404

    # Obtener el número de participantes
    number_of_participants = df_experiment['user_id'].nunique()

    # Preparar las variantes
    variants = []
    for _, row in df_experiment.iterrows():
        for variant_id, count in row['variant_counts'].items():
            variants.append({
                "id": variant_id,
                "number_of_purchases": count
            })
    print('variantes: ')
    print(variants)
    
    # Determinar el ganador
    winner = max(variants, key=lambda x: x['number_of_purchases'])['id']

    # Preparar la respuesta con el formato requerido
    response = {
        "results": {
            f"exp_{exp_id}": {
                "number_of_participants": number_of_participants,
                "winner": winner,
                "variants": sorted(variants, key=lambda x: x['id'])  # Sorting variants by id for consistency
            }
        }
    }

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
