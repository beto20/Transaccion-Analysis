from datetime import datetime
import os
import pandas as pd

def add_date_suffix(filename, date=None):
    """
    Appends _YYYYMMDD to the filename before the extension.
    
    Args:
        filename (str): Original filename (e.g. 'data.csv')
        date (datetime, optional): Date to use; defaults to today.
    
    Returns:
        str: Modified filename (e.g. 'data_20250501.csv')
    """
    if date is None:
        date = datetime.today()
        
    name, ext = os.path.splitext(filename)
    date_str = date.strftime("%Y%m%d_%H%M%S_%f")
    return f"{name}_{date_str}{ext}"

def leer_datos(path):
    return pd.read_csv(path)

def transformar_datos(df):
    df['total'] = df['cantidad'] * df['precio']
    return df

def resumir_datos(df):
    resumen = df.groupby('producto')['total'].sum().reset_index()
    return resumen

def guardar_datos(df, path):
    df.to_csv(path, index=False)