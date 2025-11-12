import os
import sys
import pandas as pd
import argparse
from datetime import datetime, date, timedelta
import logging
from utils import *
import config
from utils_api_nova import extrair_dados_api_nova_completo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def processar_dataframe_bruto(df_bruto, series_expurgo, df_ddds, df_mapa_temporal):
    logger.debug("Iniciando pré-processamento e enriquecimento do DataFrame...")
    df_proc = preprocessar_dados(df_bruto, series_expurgo)
    df_proc = aplicar_mapeamento_motivos(df_proc)
    df_proc = enriquecer_dados_com_ddds(df_proc, df_ddds)
    df_enriquecido = aplicar_mapeamento_temporal_supervisor(df_proc, df_mapa_temporal, 
                                                          use_simple_fallback=config.USE_SIMPLE_SUPERVISOR_MAPPING)
    
    return df_enriquecido

def carregar_dados_historicos():
    """
    Carrega dados históricos do parquet. Se não existir, tenta CSV como fallback.
    """
    if os.path.exists(config.ARQUIVO_DADOS_CONSOLIDADO):
        logger.info(f"Carregando dados históricos de '{config.ARQUIVO_DADOS_CONSOLIDADO}'...")
        return pd.read_parquet(config.ARQUIVO_DADOS_CONSOLIDADO)


    logger.info(f"'{config.ARQUIVO_DADOS_CONSOLIDADO}' não encontrado. Iniciando com dados vazios - tudo será extraído da API.")
    return pd.DataFrame()

def extrair_dados_api(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal):
    # SEMPRE divide em dias individuais devido às limitações da API
    data_inicio_dt = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
    data_fim_dt = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
    dias_diferenca = (data_fim_dt - data_inicio_dt).days + 1
    
    if dias_diferenca == 1:
        # Apenas 1 dia - extrai diretamente
        return extrair_dados_api_intervalo_unico(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal)
    else:
        # Múltiplos dias - divide em dias individuais
        logger.info(f"Extraindo {dias_diferenca} dias individualmente (limitação da API) - estimativa: {dias_diferenca * 3}s")
        intervalos = gerar_intervalos_datas(data_inicio_str, data_fim_str, dias_por_intervalo=1)
        
        all_dataframes = []
        total_registros = 0
        for i, (inicio_intervalo, fim_intervalo) in enumerate(intervalos, 1):
            df_intervalo = extrair_dados_api_intervalo_unico(inicio_intervalo, fim_intervalo, series_expurgo, df_ddds, df_mapa_temporal)
            if not df_intervalo.empty:
                all_dataframes.append(df_intervalo)
                total_registros += len(df_intervalo)
            
            # Log de progresso otimizado
            if i % 20 == 0 or i == len(intervalos):  # Log a cada 20 dias ou no final
                logger.info(f"Progresso: {i}/{len(intervalos)} dias | {total_registros} registros acumulados")
        
        if all_dataframes:
            df_final = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"Extração dia a dia concluída: {len(df_final)} registros de {len(intervalos)} dias")
            return df_final
        else:
            logger.info("Nenhum dado encontrado em todos os dias.")
            return pd.DataFrame()

def extrair_dados_api_intervalo_unico(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal):
    """Extrai dados da API para um único intervalo (máximo 30 dias)"""
    all_dataframes = []

    # 1. EXTRAÇÃO API PRINCIPAL (MVNOs tradicionais)
    token = autenticar_api(config.URL_BASE, config.LOGIN_API, config.SENHA_API, config.PRODUTO_API, config.DISPOSITIVO_API)
    if not token:
        logger.error("Falha na autenticação da API principal. Novos dados não serão extraídos.")
    else:
        filter_groups = config.MEUS_FILTER_GROUPS
        records = extrair_relatorio_atendidas(config.URL_BASE, token, data_inicio_str, data_fim_str, filter_groups=filter_groups)
        if records:
            df_api_bruto = pd.DataFrame(records)
            df_processado = processar_dataframe_bruto(df_api_bruto, series_expurgo, df_ddds, df_mapa_temporal)
            if not df_processado.empty:
                all_dataframes.append(df_processado)
                logger.info(f"API Principal: {len(df_processado)} registros extraídos")
        else:
            logger.info(f"API Principal: Nenhum registro encontrado para {data_inicio_str}")

    # 2. EXTRAÇÃO API NOVA (Ifood_Chip, Band_Sports)
    try:
        logger.info(f"Extraindo dados da API Nova (Ifood_Chip, Band_Sports)...")
        df_api_nova = extrair_dados_api_nova_completo(data_inicio_str, data_fim_str)

        if not df_api_nova.empty:
            # Processar dados da API nova com o mesmo pipeline
            df_api_nova_processado = processar_dataframe_bruto(df_api_nova, series_expurgo, df_ddds, df_mapa_temporal)
            if not df_api_nova_processado.empty:
                all_dataframes.append(df_api_nova_processado)
                logger.info(f"API Nova: {len(df_api_nova_processado)} registros extraídos e processados")
        else:
            logger.info(f"API Nova: Nenhum registro encontrado para {data_inicio_str}")
    except Exception as e:
        logger.warning(f"Erro ao extrair dados da API Nova (não crítico): {e}")
        # Continua mesmo se a API nova falhar

    # 3. CONSOLIDAR TODAS AS FONTES
    if all_dataframes:
        df_final = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Total consolidado: {len(df_final)} registros (API Principal + API Nova)")
        return df_final
    else:
        logger.info(f"Nenhum registro encontrado em nenhuma API para o período {data_inicio_str} até {data_fim_str}.")
        return pd.DataFrame()

def extrair_dados_api_simples(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal):
    """
    Versão simplificada da extração de API que NÃO chama verificar_e_preencher_datas_faltantes
    para evitar loops infinitos. Sempre extrai períodos únicos (usada pelo preenchimento de gaps).
    """
    return extrair_dados_api_intervalo_unico(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal)

def verificar_e_preencher_datas_faltantes(df_dados, max_gaps=7, skip_api_extraction=False):
    """
    Verifica se existem gaps de datas nos dados e tenta preenchê-los automaticamente.
    
    Args:
        df_dados: DataFrame com os dados existentes
        max_gaps: Máximo de datas faltantes a preencher (padrão: 7)
        skip_api_extraction: Se True, não faz chamadas da API (evita loops)
    """
    if df_dados.empty:
        return df_dados
    
    try:
        # Identifica o range de datas nos dados existentes
        df_dados['data_hora_contato'] = pd.to_datetime(df_dados['data_hora_contato'])
        data_min = df_dados['data_hora_contato'].min().date()
        data_max = df_dados['data_hora_contato'].max().date()
        
        # Limita data_max até ontem para não tentar preencher datas futuras
        hoje = date.today()
        ontem = hoje - timedelta(days=1)
        data_max = min(data_max, ontem)
        
        # Se data_max ficou menor que data_min, não há o que preencher
        if data_max < data_min:
            logger.info("✅ Não há datas para preencher (dados são futuros)")
            return df_dados
        
        # Identifica datas com dados
        datas_existentes = set(df_dados['data_hora_contato'].dt.date.unique())
        
        # Gera lista de todas as datas do período (até ontem no máximo)
        data_atual = data_min
        datas_esperadas = set()
        while data_atual <= data_max:
            datas_esperadas.add(data_atual)
            data_atual += timedelta(days=1)
        
        # Identifica datas faltantes
        datas_faltantes = datas_esperadas - datas_existentes
        
        if datas_faltantes:
            datas_faltantes_ordenadas = sorted(datas_faltantes)
            logger.info(f"Identificadas {len(datas_faltantes)} datas faltantes: {datas_faltantes_ordenadas[:5]}{'...' if len(datas_faltantes) > 5 else ''}")
            
            # PROTEÇÃO CONTRA LOOP: Limita o número de datas a preencher
            if len(datas_faltantes) > max_gaps:
                logger.warning(f"Muitas datas faltantes ({len(datas_faltantes)}). Limitando a {max_gaps} para evitar sobrecarga.")
                datas_faltantes_ordenadas = datas_faltantes_ordenadas[:max_gaps]
            
            # PROTEÇÃO CONTRA LOOP: Skip extração da API se solicitado
            if skip_api_extraction:
                logger.info("Pulando extração da API conforme solicitado (evita loops)")
                return df_dados
            
            # Carrega dados auxiliares UMA ÚNICA VEZ (otimização)
            logger.info("Carregando dados auxiliares para preenchimento de gaps...")
            # Usar mapeamento temporal existente (não recriar)
            df_mapa_temporal = pd.read_parquet(config.ARQUIVO_MAPEAMENTO_TEMPORAL)
            df_ddds = carregar_planilha_ddds(config.PASTA_PLANILHAS)
            series_expurgo = carregar_planilha_expurgo(config.PASTA_PLANILHAS)
            
            dados_adicionados = 0
            
            # Tenta preencher gaps pequenos automaticamente
            for data_faltante in datas_faltantes_ordenadas:
                # Não tenta preencher datas futuras
                if data_faltante > ontem:
                    logger.info(f"⏭️  Pulando data futura: {data_faltante}")
                    continue
                
                logger.info(f"Tentando preencher data faltante: {data_faltante}")
                try:
                    data_str = data_faltante.strftime('%Y-%m-%d')
                    # CHAMA FUNÇÃO ESPECÍFICA PARA EVITAR RECURSÃO (com sistema híbrido)
                    logger.debug(f"Preenchendo gap: usando extração simples para {data_str}")
                    df_novo = extrair_dados_api_simples(data_str, data_str, series_expurgo, df_ddds, df_mapa_temporal)
                    
                    if not df_novo.empty:
                        logger.info(f"✅ Dados encontrados para {data_faltante}: {len(df_novo)} registros")
                        df_dados = pd.concat([df_dados, df_novo], ignore_index=True)
                        dados_adicionados += len(df_novo)
                    else:
                        logger.info(f"⚠️  Nenhum dado encontrado na API para {data_faltante}")
                        
                except Exception as e:
                    logger.warning(f"Erro ao tentar preencher data {data_faltante}: {e}")
                    continue
            
            # Log final do preenchimento
            if dados_adicionados > 0:
                logger.info(f"🎯 Preenchimento automático concluído: {dados_adicionados} registros adicionados para {len([d for d in datas_faltantes_ordenadas if d <= ontem])} datas")
        else:
            logger.info("✅ Não foram identificadas datas faltantes no período")
            
        return df_dados
        
    except Exception as e:
        logger.error(f"Erro na verificação de datas faltantes: {e}")
        return df_dados

def verificar_e_regenerar_mapeamento_se_necessario():
    """
    Verifica se o mapeamento temporal está desatualizado e regenera se necessário.
    Retorna o DataFrame de mapeamento atualizado.
    """
    try:
        # Carregar mapeamento existente
        if not os.path.exists(config.ARQUIVO_MAPEAMENTO_TEMPORAL):
            logger.warning("Arquivo de mapeamento não encontrado. Gerando pela primeira vez...")
            return gerar_mapeamento_l5_supervisor_temporal(
                config.PASTA_MOPS_HISTORICOS,
                config.ARQUIVO_MAPEAMENTO_TEMPORAL
            )

        df_mapa = pd.read_parquet(config.ARQUIVO_MAPEAMENTO_TEMPORAL)

        # Verificar se há dados consolidados para comparar
        if os.path.exists(config.ARQUIVO_DADOS_CONSOLIDADO):
            df_dados = pd.read_parquet(config.ARQUIVO_DADOS_CONSOLIDADO)
            df_dados['data_hora_contato'] = pd.to_datetime(df_dados['data_hora_contato'])

            # Período máximo dos dados
            periodo_max_dados = df_dados['data_hora_contato'].max().date()

            # Período máximo do mapeamento
            df_mapa['data_fim_supervisor'] = pd.to_datetime(df_mapa['data_fim_supervisor']).dt.date
            periodo_max_mapeamento = df_mapa['data_fim_supervisor'].max()

            # Se os dados vão além do mapeamento por mais de 2 dias, regenerar
            if periodo_max_dados > periodo_max_mapeamento + timedelta(days=2):
                logger.warning(f"⚠️  Mapeamento desatualizado!")
                logger.warning(f"   - Dados até: {periodo_max_dados}")
                logger.warning(f"   - Mapeamento até: {periodo_max_mapeamento}")
                logger.warning(f"   - Regenerando mapeamento...")

                df_mapa_novo = gerar_mapeamento_l5_supervisor_temporal(
                    config.PASTA_MOPS_HISTORICOS,
                    config.ARQUIVO_MAPEAMENTO_TEMPORAL
                )

                if df_mapa_novo is not None and not df_mapa_novo.empty:
                    logger.info("✅ Mapeamento regenerado com sucesso")
                    return df_mapa_novo
                else:
                    logger.error("❌ Falha ao regenerar mapeamento. Usando o existente.")
                    return df_mapa

            # Verificar porcentagem de "Não Mapeados" nos últimos 7 dias
            data_inicio_check = periodo_max_dados - timedelta(days=7)
            df_ultimos_dias = df_dados[df_dados['data_hora_contato'] >= pd.Timestamp(data_inicio_check)]

            if len(df_ultimos_dias) > 0:
                pct_nao_mapeados = (df_ultimos_dias['supervisor'] == 'Não Mapeado').sum() / len(df_ultimos_dias)

                if pct_nao_mapeados > 0.3:  # Mais de 30% não mapeados
                    logger.warning(f"⚠️  {pct_nao_mapeados*100:.1f}% dos dados dos últimos 7 dias estão como 'Não Mapeado'")
                    logger.warning(f"   - Regenerando mapeamento...")

                    df_mapa_novo = gerar_mapeamento_l5_supervisor_temporal(
                        config.PASTA_MOPS_HISTORICOS,
                        config.ARQUIVO_MAPEAMENTO_TEMPORAL
                    )

                    if df_mapa_novo is not None and not df_mapa_novo.empty:
                        logger.info("✅ Mapeamento regenerado. ATENÇÃO: Execute o script de correção para reprocessar dados históricos.")
                        return df_mapa_novo

        return df_mapa

    except Exception as e:
        logger.error(f"Erro ao verificar mapeamento: {e}")
        # Em caso de erro, tenta carregar o existente
        if os.path.exists(config.ARQUIVO_MAPEAMENTO_TEMPORAL):
            return pd.read_parquet(config.ARQUIVO_MAPEAMENTO_TEMPORAL)
        else:
            return pd.DataFrame()

def executar_pipeline_principal(data_inicio_execucao, data_fim_execucao):

    # Verificar se o mapeamento temporal precisa ser regenerado
    df_mapa_temporal = verificar_e_regenerar_mapeamento_se_necessario()

    df_ddds = carregar_planilha_ddds(config.PASTA_PLANILHAS)
    series_expurgo = carregar_planilha_expurgo(config.PASTA_PLANILHAS)
    df_historico = carregar_dados_historicos()
    data_inicio_str = data_inicio_execucao.strftime('%Y-%m-%d')
    data_fim_str = data_fim_execucao.strftime('%Y-%m-%d')
    df_novos_dados = extrair_dados_api(data_inicio_str, data_fim_str, series_expurgo, df_ddds, df_mapa_temporal)
    all_dfs = [df for df in [df_historico, df_novos_dados] if not df.empty]
    if not all_dfs:
        logger.warning("Nenhum dado (histórico ou novo) disponível para processamento final. Encerrando.")
        return

    df_final_consolidado = pd.concat(all_dfs, ignore_index=True)

    # LOG DETALHADO DE DEDUPLICAÇÃO #1
    registros_antes_dedup1 = len(df_final_consolidado)
    logger.info(f"📊 DEDUPLICAÇÃO #1 - Registros ANTES: {registros_antes_dedup1:,}")

    df_final_consolidado.drop_duplicates(subset=config.COLUNAS_CHAVE_DUPLICATAS, keep='last', inplace=True)

    registros_apos_dedup1 = len(df_final_consolidado)
    registros_removidos_dedup1 = registros_antes_dedup1 - registros_apos_dedup1
    logger.info(f"📊 DEDUPLICAÇÃO #1 - Registros APÓS: {registros_apos_dedup1:,}")
    logger.info(f"📊 DEDUPLICAÇÃO #1 - Removidos: {registros_removidos_dedup1:,} ({(registros_removidos_dedup1/registros_antes_dedup1*100):.2f}%)")

    # Verificação de gaps agora é feita ANTES da extração, não depois
    # Esta função só rodará se houver pequenos gaps internos nos dados extraídos
    logger.debug("Verificando pequenos gaps internos nos dados extraídos...")
    df_final_consolidado = verificar_e_preencher_datas_faltantes(
        df_final_consolidado,
        max_gaps=3,  # Apenas gaps muito pequenos (3 dias)
        skip_api_extraction=False
    )

    # LOG DETALHADO DE DEDUPLICAÇÃO #2
    registros_antes_dedup2 = len(df_final_consolidado)
    logger.info(f"📊 DEDUPLICAÇÃO #2 - Registros ANTES: {registros_antes_dedup2:,}")

    df_final_consolidado.drop_duplicates(subset=config.COLUNAS_CHAVE_DUPLICATAS, keep='last', inplace=True)

    registros_apos_dedup2 = len(df_final_consolidado)
    registros_removidos_dedup2 = registros_antes_dedup2 - registros_apos_dedup2
    logger.info(f"📊 DEDUPLICAÇÃO #2 - Registros APÓS: {registros_apos_dedup2:,}")
    logger.info(f"📊 DEDUPLICAÇÃO #2 - Removidos: {registros_removidos_dedup2:,} ({(registros_removidos_dedup2/registros_antes_dedup2*100):.2f}%)")

    # LOG TOTAL DE DEDUPLICAÇÃO
    total_removido = registros_removidos_dedup1 + registros_removidos_dedup2
    logger.info(f"📊 DEDUPLICAÇÃO TOTAL - Removidos: {total_removido:,} registros duplicados")
    logger.info(f"📊 DEDUPLICAÇÃO - Chaves usadas: {config.COLUNAS_CHAVE_DUPLICATAS}")
    
    df_final_com_rechamadas = calcular_rechamadas(df_final_consolidado)

    # CORREÇÃO: Aplicar classificação de tipos de rechamada APÓS calcular as rechamadas
    df_final_com_rechamadas = classificar_tipos_rechamada(df_final_com_rechamadas)

    df_final_com_rechamadas["mes"] = df_final_com_rechamadas["data_hora_contato"].dt.strftime("%m-%Y")
    df_final_com_rechamadas["semana"] = df_final_com_rechamadas["data_hora_contato"].apply(get_semana_customizada)

    for col in config.COLUNAS_PARA_TEXTO:
        if col in df_final_com_rechamadas.columns:
            df_final_com_rechamadas[col] = df_final_com_rechamadas[col].astype(str)

    logger.info(f"Salvando resultado final em '{config.ARQUIVO_DADOS_CONSOLIDADO}'...")
    df_final_com_rechamadas.to_parquet(config.ARQUIVO_DADOS_CONSOLIDADO, index=False)
    logger.info(df_final_com_rechamadas['supervisor'].value_counts(dropna=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de dados de rechamada. Executa para o dia anterior por padrão ou para um período específico.")
    parser.add_argument("--data-inicio", help="Data de início no formato YYYY-MM-DD")
    parser.add_argument("--data-fim", help="Data de fim no formato YYYY-MM-DD")
    args = parser.parse_args()

    if args.data_inicio and args.data_fim:
        try:
            data_inicio_processamento = datetime.strptime(args.data_inicio, '%Y-%m-%d').date()
            data_fim_processamento = datetime.strptime(args.data_fim, '%Y-%m-%d').date()
        except ValueError:
            logger.error("ERRO: Formato de data inválido. Use YYYY-MM-DD.")
            sys.exit(1) # Sai do script indicando que um erro ocorreu
    else:
        # Lógica inteligente: verifica gaps nos dados históricos
        if os.path.exists(config.ARQUIVO_DADOS_CONSOLIDADO):
            # Carrega dados históricos para analisar gaps
            try:
                df_historico = pd.read_parquet(config.ARQUIVO_DADOS_CONSOLIDADO)
                if not df_historico.empty and 'data_hora_contato' in df_historico.columns:
                    # Analisa período completo dos dados
                    df_historico['data_hora_contato'] = pd.to_datetime(df_historico['data_hora_contato'])
                    primeira_data = df_historico['data_hora_contato'].min().date()
                    ultima_data = df_historico['data_hora_contato'].max().date()
                    
                    # Define período esperado (janeiro até ontem)
                    ano_atual = date.today().year
                    inicio_ano = date(ano_atual, 1, 1)
                    ontem = date.today() - timedelta(days=1)
                    
                    # Verifica gaps no início (antes da primeira data)
                    if primeira_data > inicio_ano:
                        # Há gap no início - extrai do início do ano até primeira data - 1
                        data_inicio_processamento = inicio_ano
                        data_fim_processamento = primeira_data - timedelta(days=1)
                        dias_gap = (data_fim_processamento - data_inicio_processamento).days + 1
                        logger.info(f"Gap no início detectado: dados começam em {primeira_data}, falta desde {inicio_ano}. Extraindo {dias_gap} dias: {data_inicio_processamento} até {data_fim_processamento}")
                    elif ultima_data < ontem:
                        # Há gap no final - extrai do dia seguinte à última data até ontem
                        data_inicio_processamento = ultima_data + timedelta(days=1)
                        data_fim_processamento = ontem
                        dias_gap = (data_fim_processamento - data_inicio_processamento).days + 1
                        logger.info(f"Gap no final detectado: última data {ultima_data}. Extraindo {dias_gap} dias: {data_inicio_processamento} até {data_fim_processamento}")
                    else:
                        # Dados completos - extrai apenas ontem
                        data_inicio_processamento = ontem
                        data_fim_processamento = ontem
                        logger.info(f"Dados completos ({primeira_data} até {ultima_data}). Extraindo apenas: {ontem}")
                else:
                    # Arquivo existe mas está vazio ou corrompido
                    raise ValueError("Arquivo histórico vazio ou sem coluna data_hora_contato")
            except Exception as e:
                # Erro ao ler arquivo - trata como se não existisse
                logger.warning(f"Erro ao analisar dados históricos: {e}. Extraindo período completo.")
                ano_atual = date.today().year
                data_inicio_processamento = date(ano_atual, 1, 1)
                data_fim_processamento = date.today() - timedelta(days=1)
                logger.info(f"Extraindo período completo: {data_inicio_processamento} até {data_fim_processamento}")
        else:
            # Arquivo não existe - extrai desde início do ano
            ano_atual = date.today().year
            data_inicio_processamento = date(ano_atual, 1, 1)
            data_fim_processamento = date.today() - timedelta(days=1)
            logger.info(f"Dados históricos NÃO encontrados. Extraindo período completo: {data_inicio_processamento} até {data_fim_processamento}")

    executar_pipeline_principal(data_inicio_processamento, data_fim_processamento)
