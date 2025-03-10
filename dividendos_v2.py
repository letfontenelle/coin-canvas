import investpy
import pandas as pd
import yfinance as yf
from datetime import datetime
import time
import os
import logging
import numpy as np
from decimal import Decimal, ROUND_HALF_UP

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dividendos_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def setup_directories():
    """Cria diretórios necessários para armazenar os dados."""
    os.makedirs('dados', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    logger.info("Diretórios de dados criados com sucesso.")

def formatar_valor(valor):
    """
    Formata um valor numérico com precisão adequada para valores monetários.
    
    Args:
        valor (float): Valor a ser formatado
        
    Returns:
        float: Valor formatado com 4 casas decimais
    """
    if pd.isna(valor) or valor is None:
        return 0.0
    
    # Usar Decimal para melhorar a precisão
    try:
        decimal_value = Decimal(str(valor))
        # Arredondar para 4 casas decimais
        rounded = decimal_value.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        return float(rounded)
    except Exception:
        logger.warning(f"Erro ao formatar valor: {valor}. Usando valor original.")
        return valor

def obter_lista_acoes():
    """
    Obtém a lista de ações brasileiras disponíveis.
    
    Returns:
        list: Lista de tickers de ações brasileiras
    """
    logger.info("Obtendo lista de ações brasileiras...")
    
    try:
        # Tentativa primária usando investpy
        acoes_brasil = investpy.get_stocks_list(country='brazil')
        logger.info(f"Total de {len(acoes_brasil)} ações encontradas via investpy.")
    except Exception as e:
        logger.error(f"Erro ao obter lista via investpy: {str(e)}")
        logger.info("Tentando método alternativo...")
        
        # Lista backup de ações mais importantes (poderia ser expandida)
        acoes_brasil = [
            "ABEV3", "PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "WEGE3", "RENT3",
            "ITSA4", "B3SA3", "MGLU3", "EGIE3", "TAEE11", "BBSE3", "FLRY3", "ELET3",
            "LREN3", "RADL3", "RAIL3", "CIEL3", "ENBR3", "CMIG4", "HYPE3", "PSSA3",
            "CPLE6", "SANB11", "TOTS3", "CSAN3", "KLBN11", "SULA11", "VIVT3", "ENGI11",
            "EQTL3", "BRAP4", "CYRE3", "CCRO3", "CSNA3", "BRKM5", "SBSP3", "TRPL4"
        ]
        logger.info(f"Usando lista de backup com {len(acoes_brasil)} ações.")
    
    return acoes_brasil

def coletar_dividendos_periodo(ano_inicio=2015, ano_fim=2024, max_retries=3, delay=0.5):
    """
    Coleta todos os dividendos pagos por ações brasileiras em um período determinado.
    
    Args:
        ano_inicio (int): Ano inicial para coleta de dividendos.
        ano_fim (int): Ano final para coleta de dividendos.
        max_retries (int): Número máximo de tentativas para cada ação
        delay (float): Tempo de espera entre requisições
    
    Returns:
        tuple: (DataFrame com todos os dividendos, DataFrame com dividendos anuais)
    """
    # Configurar diretórios
    setup_directories()
    
    # Obter lista de ações
    acoes_brasil = obter_lista_acoes()
    
    # Lista para armazenar os dados de dividendos
    dados_dividendos = []
    
    # Dicionário para armazenar os totais anuais por ação
    dividendos_anuais = {}
    
    # Contador para feedback de progresso
    total_acoes = len(acoes_brasil)
    acoes_processadas = 0
    acoes_com_dividendos = 0
    acoes_com_erro = 0
    
    # Ano atual para validação
    ano_atual = datetime.now().year
    
    # Validar período
    if ano_fim > ano_atual:
        logger.warning(f"Ano final ({ano_fim}) é maior que o ano atual. Ajustando para {ano_atual}.")
        ano_fim = ano_atual
    
    if ano_inicio > ano_fim:
        logger.error(f"Ano inicial ({ano_inicio}) é maior que o ano final ({ano_fim}). Invertendo.")
        ano_inicio, ano_fim = ano_fim, ano_inicio
    
    logger.info(f"Iniciando coleta de dividendos no período de {ano_inicio} a {ano_fim}...")
    
    # Processar todas as ações
    for ticker in acoes_brasil:
        acoes_processadas += 1
        
        # Mostrar progresso a cada 10 ações ou na última
        if acoes_processadas % 10 == 0 or acoes_processadas == total_acoes:
            progresso = (acoes_processadas/total_acoes*100)
            logger.info(f"Progresso: {acoes_processadas}/{total_acoes} ações ({progresso:.1f}%)")
        
        # Tentativas para cada ação
        for attempt in range(max_retries):
            try:
                # Adicionar sufixo .SA para ações brasileiras no Yahoo Finance
                stock = yf.Ticker(ticker+".SA")
                dividends = stock.dividends  # Histórico de dividendos
                
                # Pausar brevemente para não sobrecarregar a API
                time.sleep(delay)
                
                # Processar dividendos
                if not dividends.empty:
                    tem_dividendos_no_periodo = False
                    
                    # Para cada dividendo
                    for data, valor in dividends.items():
                        # Filtrar pelo período especificado
                        if ano_inicio <= data.year <= ano_fim:
                            # Verificar e corrigir valores problemáticos
                            if pd.isna(valor) or valor <= 0:
                                logger.warning(f"Valor inválido para {ticker} em {data.strftime('%d-%m-%Y')}: {valor}")
                                continue
                                
                            tem_dividendos_no_periodo = True
                            ano = data.year
                            
                            # Formatar o valor para evitar erros de precisão
                            valor_formatado = formatar_valor(valor)
                            
                            # Adicionar ao dataset detalhado
                            dados_dividendos.append({
                                'Data': data.strftime('%d-%m-%Y'),  # Formato dd-mm-yyyy
                                'Acao': ticker,
                                'Valor': valor_formatado,
                                'Ano': ano
                            })
                            
                            # Adicionar ao dataset anual
                            chave = (ticker, ano)
                            if chave in dividendos_anuais:
                                dividendos_anuais[chave] += valor_formatado
                            else:
                                dividendos_anuais[chave] = valor_formatado
                    
                    if tem_dividendos_no_periodo:
                        acoes_com_dividendos += 1
                        dividendos_no_periodo = len([d for d in dados_dividendos if d['Acao'] == ticker])
                        logger.info(f"{ticker}: {dividendos_no_periodo} dividendos encontrados")
                
                # Se conseguiu processar, sair do loop de tentativas
                break
                    
            except Exception as e:
                erro = str(e)
                # Se for a última tentativa, registrar como erro
                if attempt == max_retries - 1:
                    acoes_com_erro += 1
                    logger.error(f"Erro ao processar {ticker} após {max_retries} tentativas: {erro}")
                else:
                    logger.warning(f"Erro ao processar {ticker} (tentativa {attempt+1}/{max_retries}): {erro}")
                    time.sleep(delay * 2)  # Espera maior antes de tentar novamente
    
    # Criar DataFrame com os dados detalhados de dividendos
    dividendos_df = pd.DataFrame(dados_dividendos)
    
    # Criar DataFrame com os dados anuais
    dados_anuais = []
    for (ticker, ano), valor_total in dividendos_anuais.items():
        # Formatar novamente o valor total para garantir precisão
        valor_formatado = formatar_valor(valor_total)
        dados_anuais.append({
            'Ano': ano,
            'Acao': ticker,
            'Valor': valor_formatado
        })
    
    dividendos_anuais_df = pd.DataFrame(dados_anuais)
    
    # Se não houver dados, retornar DataFrames vazios
    if dividendos_df.empty:
        logger.warning(f"Nenhum dividendo encontrado no período de {ano_inicio} a {ano_fim}.")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ordenar DataFrames
    dividendos_df = dividendos_df.sort_values(by=['Acao', 'Data'])
    
    # Para o DataFrame anual, adicionar ranking por ano
    if not dividendos_anuais_df.empty:
        # Primeiro ordenar por Ano e Acao para consistência
        dividendos_anuais_df = dividendos_anuais_df.sort_values(by=['Ano', 'Acao'])
        
        # Verificar se há valores inválidos antes de fazer o ranking
        valor_min = dividendos_anuais_df['Valor'].min()
        valor_max = dividendos_anuais_df['Valor'].max()
        
        if pd.isna(valor_min) or pd.isna(valor_max):
            logger.warning("Valores NaN encontrados na coluna 'Valor'. Corrigindo...")
            dividendos_anuais_df['Valor'] = dividendos_anuais_df['Valor'].fillna(0.0)
        
        # Adicionar ranking dentro de cada ano (1 = maior pagador de dividendos do ano)
        dividendos_anuais_df['Ranking'] = dividendos_anuais_df.groupby('Ano')['Valor'].rank(method='dense', ascending=False).astype(int)
        
        # Reordenar para exibição final: por ano, e dentro de cada ano, por ranking
        dividendos_anuais_df = dividendos_anuais_df.sort_values(by=['Ano', 'Ranking'])
    
    logger.info(f"\nResumo da coleta:")
    logger.info(f"- Período analisado: {ano_inicio} a {ano_fim}")
    logger.info(f"- Total de ações processadas: {total_acoes}")
    logger.info(f"- Ações com dividendos no período: {acoes_com_dividendos}")
    logger.info(f"- Ações com erro: {acoes_com_erro}")
    logger.info(f"- Total de registros de dividendos detalhados: {len(dividendos_df)}")
    logger.info(f"- Total de registros de dividendos anuais: {len(dividendos_anuais_df)}")
    
    return dividendos_df, dividendos_anuais_df

def gerar_estatisticas(dividendos_df, dividendos_anuais_df, ano_inicio, ano_fim):
    """
    Gera estatísticas sobre os dividendos coletados.
    
    Args:
        dividendos_df (DataFrame): DataFrame com dividendos detalhados
        dividendos_anuais_df (DataFrame): DataFrame com dividendos anuais
        ano_inicio (int): Ano inicial da análise
        ano_fim (int): Ano final da análise
    """
    if dividendos_df.empty:
        logger.warning("Sem dados para gerar estatísticas.")
        return
    
    # Estatísticas gerais
    valor_medio = dividendos_df['Valor'].mean()
    valor_total = dividendos_df['Valor'].sum()
    valor_maximo = dividendos_df['Valor'].max()
    acao_maior_valor = dividendos_df.loc[dividendos_df['Valor'].idxmax(), 'Acao']
    data_maior_valor = dividendos_df.loc[dividendos_df['Valor'].idxmax(), 'Data']
    
    logger.info("\n--- ESTATÍSTICAS DOS DIVIDENDOS ---")
    logger.info(f"Valor médio de dividendos: R$ {valor_medio:.4f}")
    logger.info(f"Valor total de dividendos: R$ {valor_total:.2f}")
    logger.info(f"Maior dividendo: R$ {valor_maximo:.4f} (Ação: {acao_maior_valor}, Data: {data_maior_valor})")
    
    # Estatísticas por ano
    logger.info("\n--- DIVIDENDOS POR ANO ---")
    dividendos_por_ano = dividendos_df.groupby('Ano').agg(
        Total_Dividendos=('Valor', 'sum'),
        Media_Dividendo=('Valor', 'mean'),
        Qtd_Dividendos=('Valor', 'count'),
        Qtd_Acoes=('Acao', 'nunique')
    )
    
    for ano, row in dividendos_por_ano.iterrows():
        logger.info(f"Ano {ano}:")
        logger.info(f"  - Total de dividendos: R$ {row['Total_Dividendos']:.2f}")
        logger.info(f"  - Média por dividendo: R$ {row['Media_Dividendo']:.4f}")
        logger.info(f"  - Quantidade de dividendos: {row['Qtd_Dividendos']}")
        logger.info(f"  - Quantidade de ações pagadoras: {row['Qtd_Acoes']}")
    
    # Top 5 ações por ano
    logger.info("\n--- TOP 5 AÇÕES QUE MAIS PAGARAM DIVIDENDOS POR ANO ---")
    for ano in range(ano_inicio, ano_fim + 1):
        ano_filtrado = dividendos_anuais_df[(dividendos_anuais_df['Ano'] == ano) & (dividendos_anuais_df['Ranking'] <= 5)]
        if not ano_filtrado.empty:
            logger.info(f"\nAno {ano}:")
            for _, row in ano_filtrado.iterrows():
                logger.info(f"  {row['Ranking']}. {row['Acao']}: R$ {row['Valor']:.4f}")
    
    # Ações mais consistentes (que pagaram dividendos em mais anos)
    logger.info("\n--- AÇÕES MAIS CONSISTENTES NO PAGAMENTO DE DIVIDENDOS ---")
    anos_com_dividendos = dividendos_anuais_df.groupby('Acao').size()
    acoes_consistentes = anos_com_dividendos.sort_values(ascending=False).head(10)
    
    for acao, num_anos in acoes_consistentes.items():
        valor_total = dividendos_anuais_df[dividendos_anuais_df['Acao'] == acao]['Valor'].sum()
        logger.info(f"{acao}: Pagou dividendos em {num_anos} anos. Total acumulado: R$ {valor_total:.4f}")

def main():
    """Função principal do programa."""
    # Definir o período desejado
    ano_inicio = 2015
    ano_fim = 2024
    
    logger.info(f"Iniciando coleta de dividendos de ações brasileiras no período de {ano_inicio} a {ano_fim}...")
    
    # Definir nomes dos arquivos
    arquivo_detalhado = "dividendos.csv"
    arquivo_anual = "dividendos_ano.csv"
    
    # Coletar todos os dividendos no período (detalhados e anuais)
    dividendos_df, dividendos_anuais_df = coletar_dividendos_periodo(ano_inicio, ano_fim)
    
    # Salvar arquivos CSV se houver dados
    if not dividendos_df.empty:
        try:
            # Salvar CSV detalhado
            dividendos_df.to_csv(arquivo_detalhado, index=False, float_format='%.4f')
            logger.info(f"\nDados detalhados salvos com sucesso em {arquivo_detalhado}")
            logger.info(f"Total de {len(dividendos_df)} registros de dividendos de {dividendos_df['Acao'].nunique()} ações diferentes.")
            
            # Salvar CSV anual
            dividendos_anuais_df.to_csv(arquivo_anual, index=False, float_format='%.4f')
            logger.info(f"\nDados anuais salvos com sucesso em {arquivo_anual}")
            logger.info(f"Total de {len(dividendos_anuais_df)} registros anuais de {dividendos_anuais_df['Acao'].nunique()} ações diferentes.")
            
            # Gerar e exibir estatísticas
            gerar_estatisticas(dividendos_df, dividendos_anuais_df, ano_inicio, ano_fim)
            
        except Exception as e:
            logger.error(f"Erro ao salvar arquivos CSV: {str(e)}")
    else:
        logger.error("Não foi possível gerar os arquivos CSV pois nenhum dividendo foi encontrado.")
    
if __name__ == "__main__":
    main()