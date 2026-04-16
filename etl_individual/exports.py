import pandas as pd

DATA_FILE = '../data/PRODUCCIÓN ALTRANS S.A.S.xlsx'

def export_sheets(excel_file):
    
    df = pd.read_excel(excel_file, sheet_name=None)
    EXCLUDED_SHEETS = ['ETIQUETAS', 'Respuestas de formulario 2']
    
    for sheet_name, sheet_df in df.items():
        if sheet_name not in EXCLUDED_SHEETS:
            output_file = f'../data_sheets/{sheet_name}.csv'
            sheet_df.to_csv(output_file, index=False)
            print(f'Sheet "{sheet_name}" exported to {output_file}')
            
if __name__ == "__main__":
    export_sheets(DATA_FILE)