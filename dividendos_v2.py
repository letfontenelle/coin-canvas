import investpy
import pandas as pd
import yfinance as yf
from datetime import datetime
import time

def coletar_dividendos_periodo(ano_inicio=2015, ano_fim=2024):
    """
    Coleta todos os dividendos pagos por ações brasileiras em um período determinado.
    
    Args:
        ano_inicio (int): Ano inicial para coleta de dividendos.
        ano_fim (int): Ano final para coleta de dividendos.
    
    Returns:
        tuple: (DataFrame com todos os dividendos, DataFrame com dividendos anuais)
    """
    # Obtém a lista de todas as ações listadas no Brasil
    print(f"Obtendo lista de ações brasileiras...")
    acoes_brasil = investpy.get_stocks_list(country='brazil')
    print(f"Total de {len(acoes_brasil)} ações encontradas.")
    
    # Lista para armazenar os dados de dividendos
    dados_dividendos = []
    
    # Dicionário para armazenar os totais anuais por ação
    dividendos_anuais = {}
    
    # Contador para feedback de progresso
    total_acoes = len(acoes_brasil)
    acoes_processadas = 0
    acoes_com_dividendos = 0
    
    # Processar todas as ações
    for ticker in acoes_brasil:
        acoes_processadas += 1
        
        # Mostrar progresso a cada 10 ações ou na última
        if acoes_processadas % 10 == 0 or acoes_processadas == total_acoes:
            print(f"Progresso: {acoes_processadas}/{total_acoes} ações ({(acoes_processadas/total_acoes*100):.1f}%)")
        
        try:
            # Adicionar sufixo .SA para ações brasileiras no Yahoo Finance
            stock = yf.Ticker(ticker+".SA")
            dividends = stock.dividends  # Histórico de dividendos
            
            # Pausar brevemente para não sobrecarregar a API
            time.sleep(0.3)
            
            # Processar dividendos
            if not dividends.empty:
                tem_dividendos_no_periodo = False
                
                # Para cada dividendo
                for data, valor in dividends.items():
                    # Filtrar pelo período especificado
                    if ano_inicio <= data.year <= ano_fim:
                        tem_dividendos_no_periodo = True
                        ano = data.year
                        
                        # Adicionar ao dataset detalhado
                        dados_dividendos.append({
                            'Data': data.strftime('%d-%m-%Y'),  # Formato dd-mm-yyyy
                            'Acao': ticker,
                            'Valor': valor,
                            'Ano': ano
                        })
                        
                        # Adicionar ao dataset anual
                        chave = (ticker, ano)
                        if chave in dividendos_anuais:
                            dividendos_anuais[chave] += valor
                        else:
                            dividendos_anuais[chave] = valor
                
                if tem_dividendos_no_periodo:
                    acoes_com_dividendos += 1
                    print(f"{ticker}: {len([d for d in dados_dividendos if d['Acao'] == ticker])} dividendos encontrados")
                    
        except Exception as e:
            print(f"Erro ao processar {ticker}: {str(e)}")
            continue
    
    # Criar DataFrame com os dados detalhados de dividendos
    dividendos_df = pd.DataFrame(dados_dividendos)
    
    # Criar DataFrame com os dados anuais
    dados_anuais = []
    for (ticker, ano), valor_total in dividendos_anuais.items():
        dados_anuais.append({
            'Ano': ano,
            'Acao': ticker,
            'Valor': valor_total
        })
    
    dividendos_anuais_df = pd.DataFrame(dados_anuais)
    
    # Se não houver dados, retornar DataFrames vazios
    if dividendos_df.empty:
        print(f"Nenhum dividendo encontrado no período de {ano_inicio} a {ano_fim}.")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ordenar DataFrames
    dividendos_df = dividendos_df.sort_values(by=['Acao', 'Data'])
    
    # Para o DataFrame anual, adicionar ranking por ano
    if not dividendos_anuais_df.empty:
        # Primeiro ordenar por Ano e Acao para consistência
        dividendos_anuais_df = dividendos_anuais_df.sort_values(by=['Ano', 'Acao'])
        
        # Adicionar ranking dentro de cada ano (1 = maior pagador de dividendos do ano)
        dividendos_anuais_df['Ranking'] = dividendos_anuais_df.groupby('Ano')['Valor'].rank(method='dense', ascending=False).astype(int)
        
        # Reordenar para exibição final: por ano, e dentro de cada ano, por ranking
        dividendos_anuais_df = dividendos_anuais_df.sort_values(by=['Ano', 'Ranking'])
    
    print(f"\nResumo da coleta:")
    print(f"- Período analisado: {ano_inicio} a {ano_fim}")
    print(f"- Total de ações processadas: {total_acoes}")
    print(f"- Ações com dividendos no período: {acoes_com_dividendos}")
    print(f"- Total de registros de dividendos detalhados: {len(dividendos_df)}")
    print(f"- Total de registros de dividendos anuais: {len(dividendos_anuais_df)}")
    
    return dividendos_df, dividendos_anuais_df

def main():
    # Definir o período desejado
    ano_inicio = 2015
    ano_fim = 2024
    
    print(f"Iniciando coleta de dividendos de ações brasileiras no período de {ano_inicio} a {ano_fim}...")
    
    # Coletar todos os dividendos no período (detalhados e anuais)
    dividendos_df, dividendos_anuais_df = coletar_dividendos_periodo(ano_inicio, ano_fim)
    
    # Salvar arquivos CSV se houver dados
    if not dividendos_df.empty:
        # Salvar CSV detalhado
        nome_arquivo = "dividendos.csv"
        dividendos_df.to_csv(nome_arquivo, index=False)
        print(f"\nDados detalhados salvos com sucesso em {nome_arquivo}")
        print(f"Total de {len(dividendos_df)} registros de dividendos de {dividendos_df['Acao'].nunique()} ações diferentes.")
        
        # Salvar CSV anual
        nome_arquivo_anual = "dividendos_ano.csv"
        dividendos_anuais_df.to_csv(nome_arquivo_anual, index=False)
        print(f"\nDados anuais salvos com sucesso em {nome_arquivo_anual}")
        print(f"Total de {len(dividendos_anuais_df)} registros anuais de {dividendos_anuais_df['Acao'].nunique()} ações diferentes.")
        
        # Mostrar algumas estatísticas
        valor_medio = dividendos_df['Valor'].mean()
        valor_total = dividendos_df['Valor'].sum()
        valor_maximo = dividendos_df['Valor'].max()
        acao_maior_valor = dividendos_df.loc[dividendos_df['Valor'].idxmax(), 'Acao']
        
        print("\nEstatísticas dos dividendos detalhados:")
        print(f"Valor médio de dividendos: R$ {valor_medio:.4f}")
        print(f"Valor total de dividendos: R$ {valor_total:.2f}")
        print(f"Maior dividendo: R$ {valor_maximo:.4f} (Ação: {acao_maior_valor})")
        
        # Mostrar estatísticas por ano (resumo)
        print("\nDividendos por ano:")
        dividendos_por_ano = dividendos_df.groupby('Ano').agg(
            Total_Dividendos=('Valor', 'sum'),
            Media_Dividendo=('Valor', 'mean'),
            Qtd_Dividendos=('Valor', 'count'),
            Qtd_Acoes=('Acao', 'nunique')
        )
        print(dividendos_por_ano)
        
        # Mostrar estatísticas anuais por ação (top 5 por ano) - agora usando o ranking
        print("\nTop 5 ações que mais pagaram dividendos por ano:")
        for ano in range(ano_inicio, ano_fim + 1):
            ano_filtrado = dividendos_anuais_df[(dividendos_anuais_df['Ano'] == ano) & (dividendos_anuais_df['Ranking'] <= 5)]
            if not ano_filtrado.empty:
                print(f"\nAno {ano}:")
                for _, row in ano_filtrado.iterrows():
                    print(f"{row['Ranking']}. {row['Acao']}: R$ {row['Valor']:.4f}")
    else:
        print("Não foi possível gerar os arquivos CSV pois nenhum dividendo foi encontrado.")
    
if __name__ == "__main__":
    main()