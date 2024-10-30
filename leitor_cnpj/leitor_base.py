    
import requests
import time
import json
import pandas as pd
import logging
import os

#Configuração do Log
logging.basicConfig(filename='Log_dasConsultas.log',
                    filemode= 'w',
                    level= logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#Lendo o arquivo Excel com os CNPJs
def  ler_cnpjs_excel(entrada_excel):
    try:
        df = pd.read_excel(entrada_excel)
        cnpjs = df['CNPJ'].astype(str).str.replace(r'\D', '', regex=True).str.zfill(14).tolist() # Convertendo a coluna 'CNPJ' em lista
        return cnpjs

    except FileNotFoundError:
        print("Arquivo do excel de entrada não encontrado.")
        logging.error('Arquivo do excel de entrada não encontrado.')

#O arquivo do excel com os cnjps que serão lidos, precisa estar no format especial CEP e em uma coluna apenas, sem traço e ponto.
#Quando for rodar o código, nenhum arquivo dentro do diretório de onde está rodando o códido pode-se estar aberto, se não dá erro.
#Recomendo que a cada saldo de consulta fazer uma pausa, para não perder o contador de 3 consultas por minuto.
#O nome da coluna do arquivo do excel que vai ter os cnpjs para consulta de ser escrito com letra maiúscula CNPJ, se não dá erro.

#Sua chave  de API da ReceitaWS
api_key = '58d0b89c8477db6000a0ecc11e251280cdb060751bdeaf5ed297c16f4bf5d027'

#Função para consultar um CNPJ na API ReceitaWS
def consultar_cnpj(cnpj):
    url = f'https://receitaws.com.br/v1/cnpj/{cnpj}'
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
            print(f'Resposta não OK para cnpj {cnpj} : {response.status_code}')
            logging.warning(f"Resposta não OK para o CNPJ {cnpj}: {response.status_code}")
            return None

        #return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao consultar o CNPJ {cnpj}: {e}")
        return None

# def salvar_parcial(resultados, arquivo_json, excel_saida):
#     resultados_existente = []

#     #salvado consulta parcial em json
#     if os.path.exists(arquivo_json) and os.path.getsize(arquivo_json) > 0:
#         with open(arquivo_json, 'r', encoding='utf-8') as file:
#             try:
#                 resultados_existente = json.load(file)
#                 #print(f" Leitura de json nas consultas parciais {resultados_existente}")
#                 # Verifica se o conteúdo carregado é uma lista
#                 if not isinstance(resultados_existente, list):
#                     resultados_existente = []
#                     logging.error('O conteúdo do JSON não é uma lista. Recriando o arquivo com lista vazia.')

#             except json.JSONDecodeError:
#                 print("Arquivo não encontrado.")
#                 logging.error('Arquivo não encontrado.')    
#                 resultados_existente = []

#     # Certifique-se de que 'resultados' é uma lista
#     if isinstance(resultados, dict):
#         resultados_existente.append(resultados)  # Adiciona um dicionário único
#     elif isinstance(resultados, list):  # Se resultados é uma lista de dicionários
#         resultados_existente.extend(resultados)  # Adiciona todos os itens da lista
#     else:
#         logging.error('Resultados não é um dicionário ou uma lista de dicionários.')

#     # Determina a ordem das colunas a partir do primeiro resultado
#     #colunas_padrao = []
#     if resultados_existente:
#         if isinstance(resultados_existente[0], dict):
#             colunas_padrao = list(resultados_existente[0].keys())
#             print(f"resultados existentes {resultados_existente}")
#         else:
#             logging.error('O primeiro item em resultados_existente não é um dicionário.')

#     # Garantir que todos os itens tenham todas as colunas, preenchendo com None
#     resultados_padrozinados = []
#     for item in resultados_existente:
#         item_padronizado = {coluna: item.get(coluna, None) for coluna in colunas_padrao}
#         resultados_padrozinados.append(item_padronizado)
#         print(f"resultado padronizado {resultados_padrozinados}")

#     #salvando consulta parcial em excel
#     with open(arquivo_json, 'w', encoding='utf-8') as file:
#         json.dump(resultados_padrozinados, file, ensure_ascii=False, indent=4)
#         print(f"salvando no json {resultados_padrozinados}")
        
#     df = pd.DataFrame(resultados_padrozinados, columns=colunas_padrao)
#     df.to_excel(excel_saida, index=False)
#     print(f"salvando no excel {resultados_padrozinados}")

def consultar_cnpj_massa(cnpjs, arquivo_json, excel_saida):
    sucesso_contador = 0
    erro_contador = 0
    cnpjs_processados = set()
    resultados = []

    for i in range(0, len(cnpjs), 3):  # Processa 3 CNPJs por vez
        lote_cnpjs = cnpjs[i:i+3]

        for cnpj in lote_cnpjs:
            if cnpj in cnpjs_processados:
                continue

            resultado = consultar_cnpj(cnpj)
            cnpjs_processados.add(cnpj)  # Corrigido para adicionar o CNPJ correto
            logging.info(f"cnjps processados consulta em massa {cnpjs_processados}")

            if resultado:
                sucesso_contador += 1
                resultados.append(resultado)  # Adiciona o resultado à lista

                # Salva o resultado parcial
                #salvar_parcial(resultados, arquivo_json, excel_saida)  # Passa uma lista de um item

            else:
                erro_contador += 1

            # Log do status atual das consultas
            logging.info(f"Consultas bem sucedidas: {sucesso_contador}")
            logging.info(f"Consultas com erro: {erro_contador}")

            # Mostrando no console o status das consultas
            print(f"Consultas bem sucedidas: {sucesso_contador}")
            print(f"Consultas com erro: {erro_contador}")

        # Aguardando 1 minuto após consultar 3 CNPJs
        if (i + 3) <= len(cnpjs):
            print("Aguardando 1 minuto para a próxima consulta...")
            logging.info("Aguardando 1 minuto para a próxima consulta...")
            time.sleep(60)

    # Salva os CNPJs processados em JSON para consultas futuras
    with open('cnpjs_processados.json', 'w') as file:
        json.dump(list(cnpjs_processados), file)
        logging.info(f"cnpjs processados: {cnpjs_processados}")

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
arquivo_excel_cnpjs = r'C:\Users\LARYSSA\OneDrive - Distribuidora Sooretama\Área de Trabalho\Laryssa\projetos\leitor_cnpj\cnpj_ler_bloco1.xlsx'
arquivo_json_resultados = 'resultados_cnpj_bloco1.json'
arquivo_excel_resultados = 'resultados_cnpj_bloco1.xlsx'

#Lendo os CNPJs do arquivo Excel
cnpjs = ler_cnpjs_excel(arquivo_excel_cnpjs)
#cnpjs = '33014556009819'

#Realizando as consultas e armazenamento os resultados
resultados = consultar_cnpj_massa(cnpjs, arquivo_json_resultados, arquivo_excel_resultados)

#Salvando os resultados em JSON
salvar_resultados_json(resultados, arquivo_json_resultados)

#Convertendo o arquivo JSON para Excel saída
json_para_excel(arquivo_json_resultados, arquivo_excel_resultados)

print(f'Resultados salvos em: {arquivo_excel_resultados}')

#Observação: se o seu arquivo do cnpj em que estão os cnpjs que serão lidos não pode rodar junto com o
#script, da acesso negado.