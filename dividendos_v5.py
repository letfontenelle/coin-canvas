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
    Retorna a lista pré-definida de tickers de ações brasileiras.
    
    Returns:
        list: Lista de tickers de ações brasileiras
    """
    logger.info("Usando lista pré-definida de ações brasileiras...")
    
    # Lista de tickers extraída diretamente
    acoes_brasil = [
        "HAPV3", "CSAN3", "COGN3", "CSNA3", "BBDC4", "ABEV3", "B3SA3", "PETR4", 
        "HYPE3", "VALE3", "ITSA4", "LREN3", "ITUB4", "CPLE6", "RENT3", "MGLU3", 
        "BBAS3", "ELET3", "RADL3", "PRIO3", "ASAI3", "VAMO3", "HBSA3", "MRVE3", 
        "POMO4", "RAIZ4", "ONCO3", "BBDC3", "LWSA3", "PETR3", "VBBR3", "CMIG4", 
        "AZEV4", "JHSF3", "CCRO3", "RAIL3", "CRFB3", "UGPA3", "GGBR4", "ANIM3", 
        "BRFS3", "NTCO3", "BPAC11", "GGPS3", "EMBR3", "MRFG3", "YDUQ3", "BEEF3", 
        "EQTL3", "CVCB3", "CPLE3", "USIM5", "JBSS3", "FLRY3", "RAPT4", "MOVI3", 
        "CEAB3", "GOAU4", "TIMS3", "AMOB3", "AZUL4", "KLBN11", "ENGI11", "RDOR3", 
        "TOTS3", "ENEV3", "AURE3", "PCAR3", "WEGE3", "BRAV3", "ECOR3", "SUZB3", 
        "SMFT3", "CYRE3", "EZTC3", "BRKM5", "GMAT3", "CMIN3", "IFCM3", "CXSE3", 
        "BBSE3", "CAML3", "SIMH3", "SBFG3", "ALOS3", "SRNA3", "VIVT3", "ARML3", 
        "CBAV3", "CPFE3", "INTB3", "AZEV3", "IGTI11", "SBSP3", "MULT3", "EGIE3", 
        "ELET6", "STBP3", "LJQQ3", "MILS3", "KEPL3", "VIVA3", "PETZ3", "PDGR3", 
        "ALPA4", "SANB11", "BHIA3", "TEND3", "PSSA3", "SLCE3", "DXCO3", "IRBR3", 
        "RCSL4", "RECV3", "GRND3", "QUAL3", "BRAP4", "KLBN4", "AZZA3", "ISAE4", 
        "CLSA3", "CSMG3", "TAEE11", "GOLL4", "SAPR4", "BPAN4", "BRSR6", "OIBR3", 
        "DIRR3", "ODPV3", "AGRO3", "GUAR3", "TTEN3", "GFSA3", "VVEO3", "CURY3", 
        "POMO3", "PORT3", "SMTO3", "MYPK3", "MDNE3", "NEOE3", "PLPL3", "SAPR11", 
        "ALUP11", "EVEN3", "JALL3", "MATD3", "MLAS3", "ITUB3", "RCSL3", "PGMN3", 
        "AMER3", "VULC3", "POSI3", "CSED3", "WIZC3", "DASA3", "AMAR3", "SYNE3", 
        "CASH3", "HBRE3", "ZAMP3", "BMOB3", "ABCB4", "RANI3", "SEQL3", "SOJA3", 
        "LAVV3", "MDIA3", "ESPA3", "MTRE3", "BMGB4", "FIQE3", "MELK3", "BRBI11", 
        "JSLG3", "FESA4", "HBOR3", "LPSB3", "VLID3", "TRIS3", "PTBL3", "TFCO4", 
        "SEER3", "TUPY3", "PINE4", "KLBN3", "SHUL4", "BRST3", "ORVR3", "VITT3", 
        "ENJU3", "PNVL3", "AERI3", "LIGT3", "FRAS3", "SAPR3", "BLAU3", "DESK3", 
        "USIM3", "LOGG3", "MEAL3", "SANB3", "LEVE3", "PRNR3", "OPCT3", "TASA4", 
        "BOBR4", "PFRM3", "VIVR3", "ITSA3", "UNIP6", "AGXY3", "LUPA3", "MBLY3", 
        "RNEW4", "AMBP3", "ELMD3", "TGMA3", "TAEE4", "SANB4", "AALR3", "VTRU3", 
        "LOGN3", "ROMI3", "TECN3", "SHOW3", "CSUD3", "AZEV11", "BRKM3", "PMAM3", 
        "ALPK3", "KRSA3", "EUCA4", "PDTC3", "DEXP3", "CAMB3", "RNEW3", "DMVF3", 
        "TAEE3", "ALLD3", "SCAR3", "VSTE3", "ALPA3", "IGTI3", "CMIG3", "GOAU3", 
        "ETER3", "INEP3", "BIOM3", "RSID3", "BRAP3", "TCSA3", "FICT3", "UCAS3", 
        "CEBR6", "NGRD3", "ENGI4", "DASA11", "TRAD3", "TPIS3", "FHER3", "OIBR4", 
        "LVTC3", "LAND3", "EALT4", "OFSA3", "FRIO3", "ALUP4", "GGBR3", "PTNT4", 
        "INEP4", "BMEB4", "BEES3", "BPAC5", "HAGA3", "JFEN3", "UNIP3", "ALUP3", 
        "MGEL4", "CTSA4", "ENGI3", "OSXB3", "WHRL3", "BIED3", "DOTZ3", "PINE3", 
        "COCE5", "ATED3", "MTSA4", "TELB3", "RAPT3", "TASA3", "WHRL4", "AMAR11", 
        "BRSR3", "CLSC4", "BPAC3", "EMAE4", "PTNT3", "RDNI3", "RNEW11", "ATMP3", 
        "BAZA3", "GEPA3", "WEST3", "RPMG3", "REDE3", "HAGA4", "BGIP4", "BEES4", 
        "SNSY5", "BNBR3", "ISAE3", "RSUL4", "EPAR3", "PEAB4", "CGRA4", "NUTR3", 
        "GEPA4", "AVLL3", "RPAD3", "AHEB3", "BSLI4", "CEBR5", "EUCA3", "PLAS3", 
        "EQPA3", "BMIN4", "TELB4", "IGTI4", "NEXP3", "PEAB3", "MNDL3", "CRPG5", 
        "CRPG6", "EKTR4", "WLMM4", "EALT3", "DEXP4", "RPAD5", "PPLA11", "BAUH4", 
        "AHEB6", "CEEB3", "MRSA5B", "MNPR3", "BGIP3", "CEDO4", "CEBR3", "BSLI3", 
        "RPAD6", "CTSA3", "CGAS5", "CLSC3", "AHEB5", "BMEB3", "CTKA4", "MWET4", 
        "HETA4", "DOHL4", "ESTR4", "BIOM11", "BMKS3", "CGRA3", "AFLT3", "NORD3", 
        "TKNO4", "UNIP5", "BDLL3", "ENMT3", "HBTS5", "GPAR3", "JOPA3", "PINE11", 
        "PSVM11", "CEDO3", "BALM4", "BRKM6", "BRSR5", "BDLL4", "HOOT4", "EQMA3B", 
        "FESA3", "CPLE5", "ENMT4", "MRSA6B", "MTSA3", "PATI3", "MERC4", "WLMM3", 
        "BALM3", "EQPA5", "MRSA3B", "CGAS3", "DOHL3", "MAPT4", "CTKA3", "DTCY3", 
        "TKNO3"
    ]
    
    # Remover possíveis duplicatas
    acoes_brasil = list(set(acoes_brasil))
    logger.info(f"Total de {len(acoes_brasil)} ações na lista pré-definida.")
    
    # Mostrar amostra das ações para debug
    amostra = acoes_brasil[:10] if len(acoes_brasil) >= 10 else acoes_brasil
    logger.info(f"Amostra de ações: {', '.join(amostra)}")
    
    return acoes_brasil

def coletar_cotacoes_diarias(ticker, ano_inicio, ano_fim, max_retries=3, delay=0.5):
    """
    Coleta cotações diárias de uma ação específica para o período determinado.
    
    Args:
        ticker (str): Código da ação
        ano_inicio (int): Ano inicial para coleta
        ano_fim (int): Ano final para coleta
        max_retries (int): Número máximo de tentativas
        delay (float): Tempo de espera entre requisições
        
    Returns:
        DataFrame: DataFrame com as cotações diárias
    """
    logger.info(f"Coletando cotações diárias para {ticker}...")
    
    cotacoes_df = pd.DataFrame()
    
    # Data de início e fim
    data_inicio = f"{ano_inicio}-01-01"
    data_fim = f"{ano_fim}-12-31"
    
    for attempt in range(max_retries):
        try:
            # Usar yfinance para obter dados históricos
            stock = yf.Ticker(f"{ticker}.SA")
            hist = stock.history(start=data_inicio, end=data_fim)
            
            if hist.empty:
                logger.warning(f"{ticker}: Nenhuma cotação encontrada para o período {ano_inicio}-{ano_fim}")
                return pd.DataFrame()
            
            # Resetar o índice para transformar o 'Date' em coluna
            hist = hist.reset_index()
            
            # Adicionar ticker como coluna
            hist['Acao'] = ticker
            
            # Selecionar apenas as colunas desejadas e calcular a média
            cotacoes_df = hist[['Date', 'Acao', 'Open', 'Close']].copy()
            cotacoes_df['Media'] = (cotacoes_df['Open'] + cotacoes_df['Close']) / 2
            
            # Renomear colunas para português
            cotacoes_df = cotacoes_df.rename(columns={
                'Date': 'Data',
                'Open': 'Abertura',
                'Close': 'Fechamento'
            })
            
            # Formatar os valores para evitar problemas de precisão
            for col in ['Abertura', 'Fechamento', 'Media']:
                cotacoes_df[col] = cotacoes_df[col].apply(formatar_valor)
            
            # Converter a coluna de data para string no formato desejado
            cotacoes_df['Data'] = cotacoes_df['Data'].dt.strftime('%d-%m-%Y')
            
            # Adicionar coluna de ano para facilitar filtragem
            cotacoes_df['Ano'] = pd.to_datetime(cotacoes_df['Data'], format='%d-%m-%Y').dt.year
            
            logger.info(f"{ticker}: {len(cotacoes_df)} cotações encontradas")
            break
        
        except Exception as e:
            erro = str(e)
            if attempt == max_retries - 1:
                logger.error(f"Erro ao coletar cotações para {ticker} após {max_retries} tentativas: {erro}")
                return pd.DataFrame()
            else:
                logger.warning(f"Erro ao coletar cotações para {ticker} (tentativa {attempt+1}/{max_retries}): {erro}")
                time.sleep(delay * 2)
    
    return cotacoes_df

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
    acoes_ignoradas = 0
    
    # Ano atual para validação
    ano_atual = datetime.now().year
    
    # Lista das ações processadas (para logging)
    acoes_com_dividendos_lista = []
    
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
                        acoes_com_dividendos_lista.append(ticker)
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
    logger.info(f"- Total de ações da B3 processadas: {total_acoes}")
    logger.info(f"- Ações com dividendos no período: {acoes_com_dividendos}")
    logger.info(f"- Ações com erro: {acoes_com_erro}")
    logger.info(f"- Total de registros de dividendos detalhados: {len(dividendos_df)}")
    logger.info(f"- Total de registros de dividendos anuais: {len(dividendos_anuais_df)}")
    
    # Salvar lista de ações que pagaram dividendos para referência
    if acoes_com_dividendos > 0:
        acoes_com_dividendos_lista.sort()
        with open(os.path.join('dados', 'acoes_com_dividendos.txt'), 'w') as f:
            f.write(f"Ações que pagaram dividendos no período {ano_inicio}-{ano_fim}:\n")
            f.write("\n".join(acoes_com_dividendos_lista))
    
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

def coletar_todas_cotacoes(ano_inicio, ano_fim, max_retries=3, delay=0.5):
    """
    Coleta cotações diárias de todas as ações no período especificado
    e gera um CSV para cada ano.
    
    Args:
        ano_inicio (int): Ano inicial para coleta
        ano_fim (int): Ano final para coleta
        max_retries (int): Número máximo de tentativas
        delay (float): Tempo de espera entre requisições
    """
    # Criar diretório para armazenar os arquivos de cotações
    dir_cotacoes = os.path.join('dados', 'cotacoes')
    os.makedirs(dir_cotacoes, exist_ok=True)
    
    # Obter lista de ações
    acoes_brasil = obter_lista_acoes()
    
    # Dicionário para armazenar DataFrames de cada ano
    cotacoes_por_ano = {ano: pd.DataFrame() for ano in range(ano_inicio, ano_fim + 1)}
    
    # Contador para feedback de progresso
    total_acoes = len(acoes_brasil)
    acoes_processadas = 0
    acoes_com_cotacoes = 0
    acoes_sem_cotacoes = 0
    
    # Lista de ações com cotações (para logging)
    acoes_com_cotacoes_lista = []
    
    logger.info(f"Iniciando coleta de cotações diárias para {total_acoes} ações de {ano_inicio} a {ano_fim}...")
    
    # Processar todas as ações
    for ticker in acoes_brasil:
        acoes_processadas += 1
        
        # Mostrar progresso a cada 5 ações ou na última
        if acoes_processadas % 5 == 0 or acoes_processadas == total_acoes:
            progresso = (acoes_processadas/total_acoes*100)
            logger.info(f"Progresso cotações: {acoes_processadas}/{total_acoes} ações ({progresso:.1f}%)")
        
        # Coletar cotações para esta ação
        cotacoes_df = coletar_cotacoes_diarias(ticker, ano_inicio, ano_fim, max_retries, delay)
        
        if cotacoes_df.empty:
            acoes_sem_cotacoes += 1
            continue
        
        acoes_com_cotacoes += 1
        acoes_com_cotacoes_lista.append(ticker)
        
        # Separar as cotações por ano e adicionar ao respectivo DataFrame
        for ano in range(ano_inicio, ano_fim + 1):
            cotacoes_ano = cotacoes_df[cotacoes_df['Ano'] == ano].copy()
            
            if not cotacoes_ano.empty:
                if cotacoes_por_ano[ano].empty:
                    cotacoes_por_ano[ano] = cotacoes_ano
                else:
                    cotacoes_por_ano[ano] = pd.concat([cotacoes_por_ano[ano], cotacoes_ano])
    
    # Após processar todas as ações, verificar e salvar os dados por ano
    logger.info(f"\nSalvando arquivos de cotações por ano...")
    
    for ano, df in cotacoes_por_ano.items():
        if not df.empty:
            # Ordenar por ação e data
            df_ordenado = df.sort_values(by=['Acao', 'Data'])
            
            # Salvar arquivo para este ano
            nome_arquivo = f"cotacao_detalhamento_{ano}.csv"
            caminho_completo = os.path.join(dir_cotacoes, nome_arquivo)
            
            # Antes de salvar, verificar se há valores NaN
            valores_nan = df_ordenado.isna().sum().sum()
            if valores_nan > 0:
                logger.warning(f"Encontrados {valores_nan} valores NaN no arquivo {nome_arquivo}. Preenchendo com 0.")
                df_ordenado = df_ordenado.fillna(0)
            
            # Salvar sem a coluna de Ano (já está no nome do arquivo)
            df_ordenado.drop(columns=['Ano'], inplace=True)
            df_ordenado.to_csv(caminho_completo, index=False, float_format='%.4f', sep=';', decimal=',')

            
            logger.info(f"Arquivo {nome_arquivo} salvo com {len(df_ordenado)} cotações de {df_ordenado['Acao'].nunique()} ações.")
        else:
            logger.warning(f"Nenhuma cotação encontrada para o ano {ano}.")
    
    # Resumo da coleta
    logger.info(f"\nResumo da coleta de cotações:")
    logger.info(f"- Total de ações da B3 processadas: {total_acoes}")
    logger.info(f"- Ações com cotações: {acoes_com_cotacoes}")
    logger.info(f"- Ações sem cotações: {acoes_sem_cotacoes}")
    
    # Salvar lista de ações com cotações para referência
    if acoes_com_cotacoes > 0:
        acoes_com_cotacoes_lista.sort()
        with open(os.path.join('dados', 'acoes_com_cotacoes.txt'), 'w') as f:
            f.write(f"Ações com cotações disponíveis no período {ano_inicio}-{ano_fim}:\n")
            f.write("\n".join(acoes_com_cotacoes_lista))

def main():
    """Função principal do programa."""
    # Definir o período desejado
    ano_inicio = 2015
    ano_fim = 2024
    
    # Configurar diretórios
    setup_directories()
    
    # Parte 1: Coletar dividendos
    logger.info(f"Iniciando coleta de dividendos de ações brasileiras no período de {ano_inicio} a {ano_fim}...")
    
    # Definir nomes dos arquivos
    arquivo_detalhado = os.path.join('dados', 'dividendos.csv')
    arquivo_anual = os.path.join('dados', 'dividendos_ano.csv')
    
    # Coletar todos os dividendos no período (detalhados e anuais)
    dividendos_df, dividendos_anuais_df = coletar_dividendos_periodo(ano_inicio, ano_fim)
    
    # Salvar arquivos CSV se houver dados
    if not dividendos_df.empty:
        try:
            # Salvar CSV detalhado
            dividendos_df.to_csv(arquivo_detalhado, index=False, float_format='%.4f', sep=';', decimal=',')
            logger.info(f"\nDados detalhados salvos com sucesso em {arquivo_detalhado}")
            logger.info(f"Total de {len(dividendos_df)} registros de dividendos de {dividendos_df['Acao'].nunique()} ações diferentes.")
            
            # Salvar CSV anual
            dividendos_anuais_df.to_csv(arquivo_anual, index=False, float_format='%.4f', sep=';', decimal=',')
            logger.info(f"\nDados anuais salvos com sucesso em {arquivo_anual}")
            logger.info(f"Total de {len(dividendos_anuais_df)} registros anuais de {dividendos_anuais_df['Acao'].nunique()} ações diferentes.")
            
            # Gerar e exibir estatísticas
            gerar_estatisticas(dividendos_df, dividendos_anuais_df, ano_inicio, ano_fim)
            
        except Exception as e:
            logger.error(f"Erro ao salvar arquivos CSV de dividendos: {str(e)}")
    else:
        logger.error("Não foi possível gerar os arquivos CSV pois nenhum dividendo foi encontrado.")
    
    # Parte 2: Coletar cotações diárias
    logger.info("\n" + "="*50)
    logger.info("INICIANDO COLETA DE COTAÇÕES DIÁRIAS")
    logger.info("="*50 + "\n")
    
    try:
        coletar_todas_cotacoes(ano_inicio, ano_fim)
        logger.info("Coleta de cotações concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao coletar cotações: {str(e)}")
    
if __name__ == "__main__":
    main()