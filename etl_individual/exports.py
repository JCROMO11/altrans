import warnings
import pandas as pd
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent  # development/
DATA_FILE = _ROOT / 'data' / 'PRODUCCIÓN ALTRANS S.A.S.xlsx'
SHEETS_DIR = _ROOT / 'data_sheets'

def export_sheets(excel_file):
    # Celdas con formato "fecha" pero serial fuera de rango (valores monetarios COP
    # mal formateados en Excel). openpyxl las convierte a NaN, que es el comportamiento
    # correcto — el warning es ruido.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*is marked as a date but the serial value.*is outside the limits for dates.*",
            category=UserWarning,
            module="openpyxl",
        )
        df = pd.read_excel(excel_file, sheet_name=None)
    EXCLUDED_SHEETS = ['ETIQUETAS', 'Respuestas de formulario 2']

    SHEETS_DIR.mkdir(parents=True, exist_ok=True)
    for sheet_name, sheet_df in df.items():
        if sheet_name not in EXCLUDED_SHEETS:
            output_file = SHEETS_DIR / f'{sheet_name}.csv'
            sheet_df.to_csv(output_file, index=False)
            print(f'Sheet "{sheet_name}" exported to {output_file}')

if __name__ == "__main__":
    export_sheets(DATA_FILE)