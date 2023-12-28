# <YOUR_IMPORTS>
import os
import json
import pandas as pd  # Add this import statement
import dill
from sklearn.svm import SVC

path = os.path.expanduser('~/airflow_hw')


def predict_single(json_data, model):
    import pandas as pd
    df = pd.DataFrame.from_dict([json_data])  # Use pd instead of pandas
    y = model.predict(df)

    return {
        'id': json_data['id'],
        'price': json_data['price'],
        'pred': y[0]
    }

def predict():
    import pandas as pd
    # Определяем последнюю модель
    latest_model = sorted(os.listdir(f'{path}/data/models'))[-1]
    # Загружаем обученную модель
    with open(f'{path}/data/models/{latest_model}', 'rb') as f:
        model = dill.load(f)

    test_data_folder = f'{path}/data/test'
    predictions_folder = f'{path}/data/predictions'

    all_predictions = pd.DataFrame()  # Use pd instead of pandas

    for filename in os.listdir(test_data_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(test_data_folder, filename)
            with open(file_path, "r") as json_file:
                data = json.load(json_file)

            prediction = predict_single(data, model)

            if prediction is not None:
                import pandas as pd
                predictions_df = pd.DataFrame([prediction])
                all_predictions = pd.concat([all_predictions, predictions_df], ignore_index=True)

    output_file = os.path.join(predictions_folder, "predictions.csv")
    all_predictions.to_csv(output_file, index=False)


if __name__ == "__main__":
    predict()
