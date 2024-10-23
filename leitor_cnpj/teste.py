import pandas as pd

# Testando apenas a leitura do Excel
try:
    df = pd.read_excel(r'C:\Users\LARYSSA\OneDrive - Distribuidora Sooretama\Área de Trabalho\Laryssa\projetos\leitor_cnpj\cnpj_ler.xlsx')
    print(df.head())  # Exibir as primeiras linhas para verificação
except Exception as e:
    print(f"Erro ao ler o arquivo Excel: {e}")
