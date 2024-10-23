import requests
import time
import json
import pandas as pd
import logging

#Configuração do Log
logging.basicConfig(filename='Verificação_arquivo_excel.log',
                    level= logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#Lendo o arquivo Excel com os CNPJs
def  ler_cnpjs_excel(entrada_excel):
    try:
        df = pd.read_excel(entrada_excel)
        cnpjs = df['CNPJ'].astype(str).tolist() # Convertendo a coluna 'CNPJ' em lista
        return cnpjs

    except FileNotFoundError:
        logging.error('Arquivo do excel de entrada não encontrado.')

#Sua chave  de API da ReceitaWS
api_key = '58d0b89c8477db6000a0ecc11e251280cdb060751bdeaf5ed297c16f4bf5d027'

#Função para consultar um CNPJ na API ReceitaWS
def consultar_cnpj(cnpj):
    url = f'https://receitaws.com.br/account/cnpj{cnpj}'
    headers = {'Authorization': f'Bearer {api_key}'}
    #response = requests.get(url, headers=headers)

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Logar o status e o conteúdo da resposta
        logging.info(f"Status da resposta para o CNPJ {cnpj}: {response.status_code}")
        logging.info(f"Conteúdo da resposta: {response.text}")

         # Verifique se a resposta é válida antes de tentar transformá-la em JSON
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Resposta não OK para o CNPJ {cnpj}: {response.status_code}")
            return None

        #return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao consultar o CNPJ {cnpj}: {e}")
        return None

    # if response.status_code == 200:
    #     return response.json() #Retorna os dados em JSON se a requisição for bem sucedida
    # else:
    #     print(f'Erro ao consultar CNPJ {cnpj}: {response.status_code} - {response.text}')

    #     print(f'Status Code: {response.status_code}')
    #     print(f'Response Text: {response.text}')
    #     return None  

def consultar_cnpj_massa(cnpjs):
    resultados = []

    for i in range(0, len(cnpjs), 3): #Processa 3 cnpjs por vez
        lote_cnpjs = cnpjs[i:i+3]

        for cnpj in lote_cnpjs:
            resultado = consultar_cnpj(cnpj)
            if resultado:
                resultados.append(resultado)
        
        #Aguardando 1 minuto após consultar 3 CNPJs
        if i + 3 < len(cnpjs):
            #print("Aguardando 1 minuto para a próxima consulta...")
            logging.info("Aguardando 1 minuto para a próxima consulta...")
            time.sleep(60)

    return resultados

#Salvando os resultados em JSON
def salvar_resultados_json(resultados, arquivo_json):
    with open(arquivo_json, 'w') as file:
        json.dump(resultados, file, ensure_ascii=False, indent=4)

#Convertendo JSON para Excel
def json_para_excel(arquivo_json, excel_saida):
    with open(arquivo_json, 'r') as file:
        data = json.load(file)
        logging.info(f' resultado da conversão de json para excel {data}')

    #Extraindo os dados relevantes do JSON para um DataFrame
    df = pd.DataFrame(data)

    #Salvando o DataFrame em um arquivo Excel
    df.to_excel(excel_saida, index=False)


#Caminho das informações pelas funções

#Caminho para o arquivo Excel com os CNPJs
#arquivo_excel_cnpjs = r'C:\Users\LARYSSA\OneDrive - Distribuidora Sooretama\Área de Trabalho\Laryssa\projetos\leitor_cnpj\cnpj_ler.xlsx'


#Lendo os CNPJs do arquivo Excel
#cnpjs = ler_cnpjs_excel(arquivo_excel_cnpjs)
cnpjs = '33014556009819'

#Realizando as consultas e armazenamento os resultados
resultados = consultar_cnpj_massa(cnpjs)

#Caminho para salvar o arquivo JSON
arquivo_json_resultados = 'resultados_cnpj_j.json'

#Salvando os resultados em JSON
salvar_resultados_json(resultados, arquivo_json_resultados)

#Convertendo o arquivo JSON para Excel saída
arquivo_excel_resultados = 'resultados_cnpj_e.xlsx'
json_para_excel(arquivo_json_resultados, arquivo_excel_resultados)

print(f'Resultados salvos em: {arquivo_excel_resultados}')

#Observação: se o seu arquivo do cnpj em que estão os cnpjs que serão lidos não pode rodar junto com o
#script, da acesso negado.