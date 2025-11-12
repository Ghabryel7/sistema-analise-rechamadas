import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date
import os
import logging
import re
from constants import MVNOS_VALIDAS, PREFIXOS_MVNO_MAP, MAPEAMENTO_MOTIVOS

logger = logging.getLogger(__name__)

def formatar_segundos_para_hhmmss(segundos):
    if pd.isna(segundos) or segundos is None: return ""
    segundos = int(segundos)
    horas = segundos // 3600
    minutos = (segundos % 3600) // 60
    segundos_restantes = segundos % 60
    return f"{horas:02d}:{minutos:02d}:{segundos_restantes:02d}"

def autenticar_api(url_base_api, login, senha, produto, dispositivo):
    endpoint = f"{url_base_api}/callbox-api/login"
    headers = {"Content-Type": "application/json"}
    payload = {"login": login, "pass": senha, "product": produto, "device": dispositivo}
    try:
        response = requests.post(endpoint, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        if data and "data" in data and isinstance(data["data"], str):
            logger.debug("API: Autenticação bem-sucedida"); return data["data"]
        else:
            logger.error(f"Falha na autenticação: {data.get('message', 'Resposta inesperada da API.')}"); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de requisição durante autenticação: {e}"); return None
    except Exception as e:
        logger.critical(f"Erro crítico na autenticação: {e}", exc_info=True); return None

def extrair_relatorio_atendidas(url_base_api, token, data_inicio, data_fim, filter_groups=None, filter_status=None, filter_search="", page=1):
    endpoint = f"{url_base_api}/callbox-api/relatorios/callcenter/tab_atendidas"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    if filter_groups is None: filter_groups = []
    if filter_status is None: filter_status = []
    
    # VOLTA AO BÁSICO: usa filter_groups como no backup (funcionava)
    payload = {"filter_start_date": data_inicio, "filter_end_date": data_fim, "filter_groups": filter_groups, "filter_status": filter_status, "filter_search": filter_search, "page": page}
    logger.debug(f"Chamando API para {data_inicio} com {len(filter_groups)} grupos: {filter_groups[:3]}...")
    all_records = []
    current_page = page
    while True:
        payload["page"] = str(current_page)
        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            response_json = response.json()
            api_data = response_json.get("data", {})
            result = api_data.get("result", [])
            # Se result for False (boolean), trata como lista vazia
            records = result if isinstance(result, list) else []
            total_pages = int(api_data.get("pages", 1))
            if not records:
                logger.debug(f"API retornou 0 records na página {current_page}. Parando extração."); break
            all_records.extend(records)
            if current_page < total_pages:
                current_page += 1
            else:
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de requisição ao extrair relatório: {e}"); return []
        except Exception as e:
            logger.critical(f"Erro crítico na extração de relatório (Página {current_page}): {e}", exc_info=True); return []
    if len(all_records) > 0:
        logger.info(f"API: {len(all_records)} registros extraídos para {data_inicio}")
    else:
        logger.info(f"API: Nenhum registro encontrado para {data_inicio}")
    return all_records

def gerar_intervalos_datas(data_inicio_geral, data_fim_geral, dias_por_intervalo=30):
    start_date = datetime.strptime(data_inicio_geral, '%Y-%m-%d'); end_date = datetime.strptime(data_fim_geral, '%Y-%m-%d')
    intervals = []; current_start = start_date
    while current_start <= end_date:
        current_end = current_start + timedelta(days=dias_por_intervalo - 1)
        if current_end > end_date: current_end = end_date
        intervals.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
        current_start = current_end + timedelta(days=1)
    return intervals

# Função carregar_historico_csv removida - não é mais utilizada

def carregar_planilha_expurgo(caminho_pasta, nome_arquivo="N1 - EXPURGADOS - 2025", aba="BASE'TRONCO E HATERS"):
    caminho_completo = os.path.join(caminho_pasta, f"{nome_arquivo}.xlsx")
    if not os.path.exists(caminho_completo): logger.warning(f"Planilha de expurgo não encontrada em: {caminho_completo}."); return pd.Series(dtype=str)
    try:
        df_expurgo = pd.read_excel(caminho_completo, sheet_name=aba)
        df_expurgo.columns = df_expurgo.columns.str.strip().str.lower()
        if 'msisdn' not in df_expurgo.columns: return pd.Series(dtype=str)
        return df_expurgo["msisdn"].dropna().astype(str)
    except Exception as e:
        logger.error(f"Erro ao carregar planilha de expurgo '{caminho_completo}': {e}"); return pd.Series(dtype=str)

def carregar_planilha_ddds(caminho_pasta, nome_arquivo="LISTA DDDs.xlsx", aba="DDD"):
    caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
    if not os.path.exists(caminho_completo): logger.warning(f"Planilha de DDDs não encontrada em: {caminho_completo}."); return pd.DataFrame(columns=['ddd', 'local'])
    try:
        df_ddds = pd.read_excel(caminho_completo, sheet_name=aba)
        df_ddds = df_ddds.loc[:, ~df_ddds.columns.duplicated(keep='first')]
        df_ddds.columns = df_ddds.columns.str.strip().str.lower()
        colunas_necessarias = ['ddd', 'cidade principal', 'estado']
        if not all(col in df_ddds.columns for col in colunas_necessarias):
            logger.error(f"A planilha de DDDs não contém as colunas necessárias: {colunas_necessarias}"); return pd.DataFrame(columns=['ddd', 'local'])
        df_ddds['local'] = df_ddds['cidade principal'].astype(str) + ' - ' + df_ddds['estado'].astype(str)
        return df_ddds[['ddd', 'local']]
    except Exception as e:
        logger.error(f"Erro ao carregar planilha de DDDs '{caminho_completo}': {e}"); return pd.DataFrame(columns=['ddd', 'local'])

def gerar_mapeamento_l5_supervisor_temporal(caminho_pasta_mops_historicas, caminho_arquivo_saida):
    all_mops_dfs = []
    mop_count = 0
    if not os.path.exists(caminho_pasta_mops_historicas):
        logger.error(f"Pasta de MOPs históricas '{caminho_pasta_mops_historicas}' não encontrada."); return pd.DataFrame()
    month_abbr_map = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}
    
    # Carrega dados consolidados para análise de período ativo
    dados_consolidado_path = 'dados_consolidado.parquet'
    periodo_dados_max = datetime.now().date()
    if os.path.exists(dados_consolidado_path):
        try:
            df_dados = pd.read_parquet(dados_consolidado_path)
            df_dados['data_hora_contato'] = pd.to_datetime(df_dados['data_hora_contato'])
            periodo_dados_max = df_dados['data_hora_contato'].max().date()
            logger.info(f"Período máximo dos dados: {periodo_dados_max}")
        except Exception as e:
            logger.warning(f"Erro ao carregar dados consolidados para análise temporal: {e}")
    
    for filename in os.listdir(caminho_pasta_mops_historicas):
        # CORRIGIDO: Aceita arquivos que começam com dígito (0-9 ou 10-12) e contêm "MOP"
        if re.match(r'^\d+', filename) and filename.endswith(".xlsx") and "MOP" in filename:
            filepath = os.path.join(caminho_pasta_mops_historicas, filename)
            try:
                mop_date = None; month_match = re.search(r'\((\w{3})\)', filename, re.IGNORECASE)
                if month_match:
                    month_abbr = month_match.group(1).lower(); month_num = month_abbr_map.get(month_abbr)
                    if month_num:
                        year_match = re.search(r'(\d{4})', filename)
                        year = int(year_match.group(1)) if year_match else 2025  # Ano atual
                        mop_date = datetime(year, month_num, 1).date()
                if mop_date is None: continue
                
                logger.debug(f"Processando MOP: {filename} para data {mop_date}")
                df_mop_temp = pd.read_excel(filepath, sheet_name="MOP ", header=0); df_mop_temp.columns = df_mop_temp.columns.str.strip().str.lower()
                col_l5 = next((col for col in df_mop_temp.columns if 'l5' == col), None)
                col_matricula = next((col for col in df_mop_temp.columns if 'matricula' == col), None)
                col_supervisor = next((col for col in df_mop_temp.columns if 'supervisor' in col), None)
                
                if col_l5 and col_supervisor and col_matricula:
                    df_mop_temp_filtered = df_mop_temp[[col_l5, col_supervisor, col_matricula]].copy()
                    df_mop_temp_filtered.rename(columns={col_l5: 'l5', col_supervisor: 'supervisor', col_matricula: 'matricula'}, inplace=True)
                    for col in df_mop_temp_filtered.columns: df_mop_temp_filtered[col] = df_mop_temp_filtered[col].astype(str).str.strip()
                    df_mop_temp_filtered.replace(['', 'nan', '-'], pd.NA, inplace=True); df_mop_temp_filtered.dropna(subset=['l5', 'supervisor'], inplace=True)
                    df_mop_temp_filtered['data_inicio_supervisor'] = mop_date
                    all_mops_dfs.append(df_mop_temp_filtered)
                    mop_count += len(df_mop_temp_filtered)
                else:
                    logger.warning(f"Colunas necessárias não encontradas em {filename}: l5={col_l5}, supervisor={col_supervisor}, matricula={col_matricula}")
            except Exception as e:
                logger.error(f"Erro ao carregar MOP histórico '{filename}': {e}", exc_info=True)
    
    if not all_mops_dfs: return pd.DataFrame()
    
    df_historico_consolidado = pd.concat(all_mops_dfs, ignore_index=True)
    logger.info(f"MOPs processados: {len(all_mops_dfs)} arquivos, {len(df_historico_consolidado)} registros consolidados")
    
    df_historico_consolidado['matricula'] = df_historico_consolidado['matricula'].fillna('SEM_MATRICULA_' + df_historico_consolidado['l5'])
    df_historico_consolidado.sort_values(by=['matricula', 'data_inicio_supervisor'], inplace=True)
    df_historico_consolidado['data_inicio_supervisor'] = pd.to_datetime(df_historico_consolidado['data_inicio_supervisor']).dt.date
    
    # CORREÇÃO PRINCIPAL: Lógica melhorada para data_fim_supervisor
    def calcular_data_fim_inteligente(group):
        group = group.sort_values('data_inicio_supervisor').copy()
        group['data_fim_supervisor'] = None

        for i in range(len(group)):
            if i < len(group) - 1:
                # Se há próximo registro, termina 1 dia antes
                group.iloc[i, group.columns.get_loc('data_fim_supervisor')] = group.iloc[i+1]['data_inicio_supervisor'] - timedelta(days=1)
            else:
                # CORREÇÃO: Último registro se estende até o período máximo dos dados
                ultima_data_inicio = group.iloc[i]['data_inicio_supervisor']

                # Vai até o último dia do mês do MOP
                ano_mop = ultima_data_inicio.year
                mes_mop = ultima_data_inicio.month

                # Calcular último dia do mês
                if mes_mop == 12:
                    ultimo_dia_mes = date(ano_mop + 1, 1, 1) - timedelta(days=1)
                else:
                    ultimo_dia_mes = date(ano_mop, mes_mop + 1, 1) - timedelta(days=1)

                # NOVA LÓGICA: Se não temos MOP novo, estende até o período máximo dos dados
                # Isso garante que outubro (sem MOP) use supervisores de setembro
                data_fim_sugerida = max(ultimo_dia_mes, periodo_dados_max)
                group.iloc[i, group.columns.get_loc('data_fim_supervisor')] = data_fim_sugerida

        return group
    
    df_historico_consolidado = df_historico_consolidado.groupby('matricula', group_keys=False).apply(calcular_data_fim_inteligente)
    
    # Adiciona extensão automática para supervisores que ainda estão ativos
    df_historico_consolidado['data_fim_supervisor'] = pd.to_datetime(df_historico_consolidado['data_fim_supervisor']).dt.date
    
    df_mapeamento_final = df_historico_consolidado[['l5', 'supervisor', 'data_inicio_supervisor', 'data_fim_supervisor']].drop_duplicates(subset=['l5', 'data_inicio_supervisor'])
    
    logger.info(f"Mapeamento final: {len(df_mapeamento_final)} registros, {df_mapeamento_final['l5'].nunique()} L5s, {df_mapeamento_final['supervisor'].nunique()} supervisores")
    
    try:
        df_mapeamento_final.to_parquet(caminho_arquivo_saida, index=False)
        logger.info(f"Mapeamento salvo em: {caminho_arquivo_saida}")
    except Exception as e:
        logger.error(f"Erro ao salvar mapeamento em Parquet: {e}", exc_info=True)
    return df_mapeamento_final

def carregar_mapeamento_temporal(caminho_arquivo):
    if not os.path.exists(caminho_arquivo): logger.warning(f"Arquivo de mapeamento temporal '{caminho_arquivo}' não encontrado."); return pd.DataFrame()
    try:
        df = pd.read_parquet(caminho_arquivo)
        df['data_inicio_supervisor'] = pd.to_datetime(df['data_inicio_supervisor']); df['data_fim_supervisor'] = pd.to_datetime(df['data_fim_supervisor'])
    except Exception as e:
        logger.error(f"Erro ao carregar o mapeamento temporal de '{caminho_arquivo}': {e}"); return pd.DataFrame()

def preprocessar_dados(df_bruto, series_expurgo):
    if df_bruto.empty: 
        logger.info("DataFrame bruto está vazio")
        return pd.DataFrame()
    df_bruto.columns = df_bruto.columns.str.strip().str.lower()
    colunas_para_renomear = {"date": "data_hora_contato", "protocol": "protocolo", "origin": "origem", "callcentergroup": "mvno", "identification": "motivo_original", "agent": "l5_agente", "nameagent": "nome_agente", "waitingtime": "tempo_espera", "servicetime": "tempo_atendimento", "calltime": "tempo_ligacao_total", "status": "status_ligacao"}
    df_bruto = df_bruto.rename(columns=colunas_para_renomear, errors='ignore')
    
    # FILTRAGEM MANUAL TEMPORAL: Remove dados de grupos não autorizados usando filtro adaptativo por data
    import config
    if 'mvno' in df_bruto.columns and 'data_hora_contato' in df_bruto.columns:
        registros_antes = len(df_bruto)
        
        # Aplica filtro diferente baseado na data de cada registro
        mask_filtro = df_bruto.apply(lambda row: filtrar_registro_por_data(row), axis=1)
        df_bruto = df_bruto[mask_filtro].copy()
        
        registros_apos = len(df_bruto)
        logger.info(f"Filtragem temporal aplicada: {registros_antes} -> {registros_apos} registros ({registros_antes - registros_apos} removidos)")
    else:
        logger.warning("Coluna 'mvno' ou 'data_hora_contato' não encontrada para filtragem. Mantendo todos os dados.")
    
    # FILTROS PYTHON - Aplicar filtros de filas válidas e exclusão de filas indesejadas
    import config
    registros_antes = len(df_bruto)
    
    # 1. Filtrar por grupos válidos (se especificado)
    if hasattr(config, 'GRUPOS_PARA_FILTRAR_PYTHON') and config.GRUPOS_PARA_FILTRAR_PYTHON:
        grupos_validos = config.GRUPOS_PARA_FILTRAR_PYTHON
        # A API usa 'callCenterGroup' que é renomeado para 'mvno'
        if 'callcentergroup' in df_bruto.columns:
            df_bruto = df_bruto[df_bruto['callcentergroup'].isin(grupos_validos)].copy()
        elif 'mvno' in df_bruto.columns:
            df_bruto = df_bruto[df_bruto['mvno'].isin(grupos_validos)].copy()
    
    # 2. Excluir filas indesejadas (aplicar em todos os campos relevantes)
    if hasattr(config, 'FILAS_PARA_EXCLUIR') and config.FILAS_PARA_EXCLUIR:
        filas_excluir = config.FILAS_PARA_EXCLUIR
        
        # Excluir no campo callCenterGroup (principal)
        if 'callcentergroup' in df_bruto.columns:
            df_bruto = df_bruto[~df_bruto['callcentergroup'].isin(filas_excluir)].copy()
        
        # Excluir no campo MVNO mapeado
        if 'mvno' in df_bruto.columns:
            df_bruto = df_bruto[~df_bruto['mvno'].isin(filas_excluir)].copy()
            
        # Excluir no campo identification (onde chegam as filas específicas como Age_*)
        if 'identification' in df_bruto.columns:
            # Filtrar por prefixos das filas indesejadas
            mask_identification = pd.Series(True, index=df_bruto.index)
            for fila_excluir in filas_excluir:
                # Excluir registros que começam com o prefixo da fila indesejada
                mask_identification &= ~df_bruto['identification'].str.startswith(fila_excluir, na=False)
            df_bruto = df_bruto[mask_identification].copy()
        
        # Excluir no motivo_original (onde também podem aparecer)
        if 'motivo_original' in df_bruto.columns:
            mask_motivo = pd.Series(True, index=df_bruto.index)  
            for fila_excluir in filas_excluir:
                mask_motivo &= ~df_bruto['motivo_original'].str.startswith(fila_excluir, na=False)
            df_bruto = df_bruto[mask_motivo].copy()
    
    registros_apos = len(df_bruto)
    if registros_antes != registros_apos:
        logger.info(f"Filtros aplicados: {registros_antes} -> {registros_apos} registros ({registros_antes - registros_apos} removidos)")
    else:
        logger.info("Filtros aplicados: nenhum registro removido")
    
    # Continua com o resto do processamento
    if 'l5_agente' in df_bruto.columns: 
        df_bruto['l5_agente'] = df_bruto['l5_agente'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if 'data_hora_contato' in df_bruto.columns:
        df_bruto["data_hora_contato"] = pd.to_datetime(df_bruto["data_hora_contato"], errors='coerce')
        df_bruto.dropna(subset=['data_hora_contato', 'protocolo'], inplace=True)
    else: 
        logger.error("Coluna 'data_hora_contato' não encontrada após renomeação")
        return pd.DataFrame()
    
    def convert_time_to_seconds(time_value):
        if pd.isna(time_value): return 0
        try:
            if isinstance(time_value, str) and ':' in time_value:
                parts = time_value.split(':'); h, m, s = (map(int, parts) if len(parts) == 3 else (0, *map(int, parts))); return h * 3600 + m * 60 + s
            return pd.to_numeric(time_value, errors='coerce')
        except: return pd.to_numeric(time_value, errors='coerce')
    
    for col in ["tempo_espera", "tempo_atendimento", "tempo_ligacao_total"]:
        if col in df_bruto.columns: df_bruto[col] = df_bruto[col].apply(convert_time_to_seconds).fillna(0)
        else: df_bruto[col] = 0
    
    if 'mvno' in df_bruto.columns and 'motivo_original' in df_bruto.columns:
        mvno_corrigida = []
        motivo_corrigido = []
        
        for idx, row in df_bruto.iterrows():
            try:
                mvno_bruto = str(row.get('mvno', '')).strip()
                motivo = str(row.get('motivo_original', '')).strip() if pd.notna(row.get('motivo_original')) else ''
                
                # FILTRO: Remove registros com grupo vazio (chamadas internacionais não classificadas, transferências, etc)
                if not mvno_bruto or mvno_bruto == '':
                    continue  # Pula este registro
                
                # Primeiro: verifica correspondência exata no mapeamento
                if mvno_bruto in PREFIXOS_MVNO_MAP:
                    mvno_corrigida.append(PREFIXOS_MVNO_MAP[mvno_bruto])
                    motivo_corrigido.append(motivo)
                    continue
                
                # Segundo: se MVNO está nas válidas, mantém como está
                if mvno_bruto in MVNOS_VALIDAS:
                    mvno_corrigida.append(mvno_bruto)
                    motivo_corrigido.append(motivo)
                    continue
                
                # Terceiro: procura por prefixos conhecidos (para casos especiais)
                encontrado = False
                for prefixo, mvno_correta in PREFIXOS_MVNO_MAP.items():
                    if prefixo.endswith('_') and mvno_bruto.startswith(prefixo):
                        motivo_extraido = mvno_bruto[len(prefixo):]
                        motivo_final = motivo_extraido if not motivo or motivo == '-' else motivo
                        mvno_corrigida.append(mvno_correta)
                        motivo_corrigido.append(motivo_final)
                        encontrado = True
                        break
                
                # Quarto: tentar fallback usando motivo_original se MVNO falhou
                if not encontrado and motivo:
                    # Verificar se o motivo_original é uma fila válida
                    if motivo in PREFIXOS_MVNO_MAP:
                        mvno_corrigida.append(PREFIXOS_MVNO_MAP[motivo])
                        motivo_corrigido.append(motivo)
                        encontrado = True
                    else:
                        # Verificar se motivo_original começa com prefixo conhecido
                        for prefixo, mvno_correta in PREFIXOS_MVNO_MAP.items():
                            if prefixo.endswith('_') and motivo.startswith(prefixo):
                                mvno_corrigida.append(mvno_correta)
                                motivo_corrigido.append(motivo)
                                encontrado = True
                                break
                
                if not encontrado:
                    mvno_corrigida.append("MVNO Não Identificada")
                    motivo_corrigido.append(motivo)
                    
            except Exception as e:
                logger.error(f"Erro em corrigir_mvno_e_motivo: {e}")
                mvno_corrigida.append("MVNO Não Identificada")
                motivo_corrigido.append("")
        
        # Reconstrói DataFrame apenas com registros válidos
        registros_antes = len(df_bruto)
        indices_validos = [i for i, mvno in enumerate(mvno_corrigida) if mvno != "SKIP"]
        df_bruto = df_bruto.iloc[indices_validos].copy()
        df_bruto['mvno'] = mvno_corrigida
        df_bruto['motivo_original'] = motivo_corrigido
        
        registros_apos = len(df_bruto)
        if registros_antes != registros_apos:
            logger.info(f"Filtro de grupos vazios: {registros_antes} -> {registros_apos} registros ({registros_antes - registros_apos} removidos)")
    
    if not series_expurgo.empty and "origem" in df_bruto.columns:
        df_bruto["is_expurgado"] = df_bruto["origem"].astype(str).isin(series_expurgo)
    else: 
        df_bruto["is_expurgado"] = False
    
    if 'origem' in df_bruto.columns: 
        df_bruto["ddd"] = df_bruto["origem"].astype(str).str[:2]
    else: 
        df_bruto["ddd"] = None
    
    return df_bruto

def filtrar_registro_por_data(row):
    """Aplica filtro apropriado baseado na data do registro"""
    try:
        import config
        from datetime import datetime
        
        # Converte data_hora_contato para string de data
        data_registro = row['data_hora_contato']
        if hasattr(data_registro, 'strftime'):
            data_str = data_registro.strftime('%Y-%m-%d')
        else:
            data_str = str(data_registro)[:10]  # Pega apenas YYYY-MM-DD
        
        # Obtém lista de grupos apropriada para a data
        grupos_validos = config.get_filter_groups_por_data(data_str)
        
        # Retorna True se o grupo do registro está na lista válida
        return row['mvno'] in grupos_validos
    except Exception as e:
        # Em caso de erro, mantém o registro (mais seguro)
        return True

def aplicar_mapeamento_motivos(df):
    if df.empty: return pd.DataFrame()
    if "motivo_original" not in df.columns:
        df["motivo_categoria"] = None; return df
    df["motivo_categoria"] = df["motivo_original"].map(MAPEAMENTO_MOTIVOS).fillna(df["motivo_original"])
    return df

def enriquecer_dados_com_ddds(df, df_ddds):
    if df.empty or df_ddds.empty:
        df['local'] = 'Não Identificado'; return df
    df['ddd'] = df['ddd'].astype(str); df_ddds['ddd'] = df_ddds['ddd'].astype(str)
    df = pd.merge(df, df_ddds, on='ddd', how='left')
    df['local'] = df['local'].fillna('Não Identificado')
    return df

def aplicar_mapeamento_temporal_supervisor_simples(df_dados, df_mapa_temporal):
    """Versão simplificada e confiável do mapeamento temporal (baseada no backup)"""
    if df_dados.empty or df_mapa_temporal.empty:
        df_dados['supervisor'] = 'Não Mapeado'
        return df_dados

    df_mapa_temp = df_mapa_temporal.copy()
    df_mapa_temp['l5'] = df_mapa_temp['l5'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_mapa_temp['data_inicio_supervisor'] = pd.to_datetime(df_mapa_temp['data_inicio_supervisor'], format='mixed').dt.date
    df_mapa_temp['data_fim_supervisor'] = pd.to_datetime(df_mapa_temp['data_fim_supervisor'], format='mixed').dt.date
    
    df_dados_temp = df_dados.copy()
    df_dados_temp['l5_agente'] = df_dados_temp['l5_agente'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_dados_temp['data_contato_date'] = pd.to_datetime(df_dados_temp['data_hora_contato']).dt.date
    
    def encontrar_supervisor_simples(row):
        regras_possiveis = df_mapa_temp[df_mapa_temp['l5'] == row['l5_agente']]
        if regras_possiveis.empty:
            return 'Não Mapeado'

        # Filtra regras que se aplicam à data
        regras_aplicaveis = regras_possiveis[
            (regras_possiveis['data_inicio_supervisor'] <= row['data_contato_date']) &
            (regras_possiveis['data_fim_supervisor'] >= row['data_contato_date'])
        ]

        if regras_aplicaveis.empty:
            # FALLBACK: Se não encontrou no período, usa o último supervisor conhecido
            # Pega a regra com data_fim_supervisor mais recente (último supervisor)
            regra_mais_recente = regras_possiveis.loc[regras_possiveis['data_fim_supervisor'].idxmax()]
            return regra_mais_recente['supervisor']

        # Se há múltiplas regras (sobreposições), escolhe a mais recente (data_inicio mais tarde)
        regra_mais_recente = regras_aplicaveis.loc[regras_aplicaveis['data_inicio_supervisor'].idxmax()]
        return regra_mais_recente['supervisor']
    
    df_dados['supervisor'] = df_dados_temp.apply(encontrar_supervisor_simples, axis=1)
    return df_dados

def aplicar_mapeamento_temporal_supervisor(df_dados, df_mapa_temporal, use_simple_fallback=True):
    """
    Mapeamento temporal híbrido:
    - Tenta primeiro o mapeamento inteligente
    - Se muitos não mapeados, usa fallback simples (baseado no backup funcionando)
    """
    if df_dados.empty or df_mapa_temporal.empty:
        df_dados['supervisor'] = 'Não Mapeado'
        return df_dados
    
    # Se solicitado, usa diretamente o mapeamento simples
    if use_simple_fallback:
        logger.info("Usando mapeamento simples de supervisor (baseado no backup)")
        return aplicar_mapeamento_temporal_supervisor_simples(df_dados, df_mapa_temporal)
    
    # Tenta mapeamento inteligente primeiro
    df_mapa_temp = df_mapa_temporal.copy()
    df_mapa_temp['l5'] = df_mapa_temp['l5'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_mapa_temp['data_inicio_supervisor'] = pd.to_datetime(df_mapa_temp['data_inicio_supervisor']).dt.date
    df_mapa_temp['data_fim_supervisor'] = pd.to_datetime(df_mapa_temp['data_fim_supervisor']).dt.date
    
    df_dados_temp = df_dados.copy()
    df_dados_temp['l5_agente'] = df_dados_temp['l5_agente'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_dados_temp['data_contato_date'] = pd.to_datetime(df_dados_temp['data_hora_contato']).dt.date
    
    # Análise para L5s ausentes e criação de mapeamentos inteligentes
    l5s_dados = set(df_dados_temp['l5_agente'].unique())
    l5s_mapeamento = set(df_mapa_temp['l5'].unique())
    l5s_ausentes = l5s_dados - l5s_mapeamento
    
    if l5s_ausentes:
        logger.info(f"Encontrados {len(l5s_ausentes)} L5s sem mapeamento: {list(l5s_ausentes)[:10]}")
        
        # Cria mapeamentos inteligentes baseados em padrões
        novos_mapeamentos = []
        for l5_ausente in l5s_ausentes:
            dados_l5 = df_dados_temp[df_dados_temp['l5_agente'] == l5_ausente]
            if not dados_l5.empty:
                data_min = dados_l5['data_contato_date'].min()
                data_max = dados_l5['data_contato_date'].max()
                nome_agente = dados_l5['nome_agente'].iloc[0] if 'nome_agente' in dados_l5.columns else 'Desconhecido'
                
                # Tenta inferir supervisor baseado em padrões de L5 similares ou hierarquia
                supervisor_inferido = inferir_supervisor_por_padrao(l5_ausente, nome_agente, df_mapa_temp)
                
                novo_mapeamento = {
                    'l5': l5_ausente,
                    'supervisor': supervisor_inferido,
                    'data_inicio_supervisor': data_min,
                    'data_fim_supervisor': max(data_max, datetime.now().date())
                }
                novos_mapeamentos.append(novo_mapeamento)
                logger.info(f"  Criado mapeamento para L5 {l5_ausente} ({nome_agente}) -> {supervisor_inferido}")
        
        if novos_mapeamentos:
            df_novos = pd.DataFrame(novos_mapeamentos)
            df_mapa_temp = pd.concat([df_mapa_temp, df_novos], ignore_index=True)
    
    def encontrar_supervisor_inteligente(row):
        l5_agente = row['l5_agente']
        data_contato = row['data_contato_date']
        
        regras_possiveis = df_mapa_temp[df_mapa_temp['l5'] == l5_agente]
        if regras_possiveis.empty:
            return 'Não Mapeado'
        
        # 1. Busca match exato no período
        for _, regra in regras_possiveis.iterrows():
            if regra['data_inicio_supervisor'] <= data_contato <= regra['data_fim_supervisor']:
                return regra['supervisor']
        
        # 2. Se não encontrou match exato, busca o mais próximo
        regras_possiveis = regras_possiveis.copy()
        regras_possiveis['distancia_inicio'] = abs((regras_possiveis['data_inicio_supervisor'] - data_contato).apply(lambda x: x.days))
        regras_possiveis['distancia_fim'] = abs((regras_possiveis['data_fim_supervisor'] - data_contato).apply(lambda x: x.days))
        regras_possiveis['distancia_min'] = regras_possiveis[['distancia_inicio', 'distancia_fim']].min(axis=1)
        
        # Pega a regra mais próxima temporalmente (máximo 90 dias de distância)
        regra_mais_proxima = regras_possiveis.loc[regras_possiveis['distancia_min'].idxmin()]
        if regra_mais_proxima['distancia_min'] <= 90:  # Máximo 3 meses de distância
            logger.debug(f"Mapeamento próximo para L5 {l5_agente} em {data_contato}: {regra_mais_proxima['supervisor']} (distância: {regra_mais_proxima['distancia_min']} dias)")
            return regra_mais_proxima['supervisor']
        
        return 'Não Mapeado'
    
    df_dados['supervisor'] = df_dados_temp.apply(encontrar_supervisor_inteligente, axis=1)
    
    # Log estatísticas
    total_nao_mapeados = (df_dados['supervisor'] == 'Não Mapeado').sum()
    logger.info(f"Após mapeamento inteligente: {total_nao_mapeados} registros ainda não mapeados ({total_nao_mapeados/len(df_dados)*100:.1f}%)")
    
    return df_dados

def inferir_supervisor_por_padrao(l5, nome_agente, df_mapa_temp):
    """
    Infere supervisor baseado em padrões de L5 similares, hierarquia ou regras de negócio
    """
    try:
        l5_num = int(l5)
        
        # Regras específicas baseadas em ranges de L5
        if 1000 <= l5_num <= 1999:
            # Range 1000: geralmente supervisão especial ou temporária
            supervisores_1000 = df_mapa_temp[df_mapa_temp['l5'].str.match(r'^1\d{3}$', na=False)]['supervisor'].value_counts()
            if not supervisores_1000.empty:
                return supervisores_1000.index[0]
            return 'SUPERVISOR RANGE 1000'
            
        elif 2000 <= l5_num <= 2999:
            # Range 2000: operadores principais
            supervisores_2000 = df_mapa_temp[df_mapa_temp['l5'].str.match(r'^2\d{3}$', na=False)]['supervisor'].value_counts()
            if not supervisores_2000.empty:
                return supervisores_2000.index[0]
            return 'Não Mapeado'  # L5s sem mapeamento devem ser "Não Mapeado"
            
        elif 5000 <= l5_num <= 5999:
            # Range 5000
            return 'SUPERVISOR RANGE 5000'
            
        elif 6000 <= l5_num <= 6999:
            # Range 6000
            return 'SUPERVISOR RANGE 6000'
            
        elif 8000 <= l5_num <= 8999:
            # Range 8000: possivelmente testes
            if 'teste' in nome_agente.lower() or l5 == '8888':
                return 'SUPERVISOR DE TESTES'
            return 'SUPERVISOR RANGE 8000'
            
        elif 9000 <= l5_num <= 9999:
            # Range 9000: possivelmente supervisão especial
            supervisores_9000 = df_mapa_temp[df_mapa_temp['l5'].str.match(r'^9\d{3}$', na=False)]['supervisor'].value_counts()
            if not supervisores_9000.empty:
                return supervisores_9000.index[0]
            return 'SUPERVISOR RANGE 9000'
            
        elif 10000 <= l5_num <= 99999:
            # Ranges altos: supervisão especial ou temporária
            return 'SUPERVISOR ESPECIAL'
    
    except (ValueError, TypeError):
        pass
    
    # Se não conseguiu inferir por range, retorna Não Mapeado
    return 'Não Mapeado'

def get_semana_customizada(date_obj):
    try: dt = pd.to_datetime(date_obj).date()
    except (ValueError, TypeError): return "Data Inválida"
    reference_friday = date(2025, 6, 13); reference_week_number = 25
    delta_days = (dt - reference_friday).days; week_offset = delta_days // 7
    custom_week_number = reference_week_number + week_offset
    return f"S{custom_week_number}"

def calcular_rechamadas(df_processado):
    if df_processado.empty:
        return pd.DataFrame()
    df_sorted = df_processado.sort_values(by=['origem', 'data_hora_contato'], ascending=True).copy()
    time_since_last_call = df_sorted.groupby('origem')['data_hora_contato'].diff()
    df_sorted['is_rechamada'] = (time_since_last_call <= pd.Timedelta(hours=24))
    df_sorted['data_chamada_original'] = df_sorted.groupby('origem')['data_hora_contato'].shift(1)
    df_sorted['rechamada_atribuida_l5'] = df_sorted.groupby('origem')['l5_agente'].shift(1)
    df_sorted['tempo_atendimento_original'] = df_sorted.groupby('origem')['tempo_atendimento'].shift(1)
    if 'protocolo' in df_sorted.columns:
        df_sorted['protocolo_chamada_original'] = df_sorted.groupby('origem')['protocolo'].shift(1)
    return df_sorted

def classificar_tipos_rechamada(df):
    if df.empty or 'is_rechamada' not in df.columns:
        df['tipo_rechamada'] = pd.NA
        return df

    df_sorted = df.sort_values(by=['origem', 'data_hora_contato'], ascending=True).copy()
    df_sorted['last_motivo'] = df_sorted.groupby('origem')['motivo_categoria'].shift(1)
    df_sorted['tipo_rechamada'] = pd.NA

    # Melhor lógica: verifica se ambos os motivos são válidos antes de comparar
    is_rechamada = df_sorted['is_rechamada'] == True
    motivo_atual_valido = df_sorted['motivo_categoria'].notna() & (df_sorted['motivo_categoria'] != '')
    motivo_anterior_valido = df_sorted['last_motivo'].notna() & (df_sorted['last_motivo'] != '')

    # Com motivo: rechamada E ambos motivos válidos E motivos iguais
    com_motivo_mask = is_rechamada & motivo_atual_valido & motivo_anterior_valido & (df_sorted['motivo_categoria'] == df_sorted['last_motivo'])

    # Sem motivo: rechamada E ambos motivos válidos E motivos diferentes
    sem_motivo_mask = is_rechamada & motivo_atual_valido & motivo_anterior_valido & (df_sorted['motivo_categoria'] != df_sorted['last_motivo'])

    # Aplicar classificações
    df_sorted.loc[com_motivo_mask, 'tipo_rechamada'] = 'Com Motivo'
    df_sorted.loc[sem_motivo_mask, 'tipo_rechamada'] = 'Sem Motivo'

    # Para rechamadas que não puderam ser classificadas (motivos inválidos), classificar como "Sem Motivo" por padrão
    rechamadas_nao_classificadas = is_rechamada & df_sorted['tipo_rechamada'].isna()
    df_sorted.loc[rechamadas_nao_classificadas, 'tipo_rechamada'] = 'Sem Motivo'

    df_sorted.drop(columns=['last_motivo'], inplace=True, errors='ignore')
    return df_sorted

def gerar_tabela_desempenho_atendente(df_filtrado, format_output=True, filtros_adicionais=None):
    if df_filtrado is None or df_filtrado.empty:
        return pd.DataFrame()
    
    # PRIMEIRA: Calcular rechamadas ANTES de aplicar filtros
    df_rechamadas = df_filtrado[df_filtrado['is_rechamada'] == True].copy()
    logger.info(f"Tabela1 - Rechamadas do período: {len(df_rechamadas)} registros")
    if df_rechamadas.empty:
        return pd.DataFrame()
    
    # CORREÇÃO MAJOR: NÃO filtrar o dataset por agentes/supervisores
    # Isso será feito DEPOIS no filtro de rechamadas, igual à Tabela 2
    df_para_contagem = df_filtrado.copy()

    # Aplicar filtros no df_para_contagem para obter lista correta de agentes filtrados
    if filtros_adicionais:
        for coluna, valores in filtros_adicionais.items():
            if valores and coluna in df_para_contagem.columns:
                df_para_contagem = df_para_contagem[df_para_contagem[coluna].isin(valores)]
                logger.info(f"Tabela1 - Aplicado filtro {coluna}={valores}: {len(df_para_contagem)} registros restantes")

    df_filtrado_final = df_para_contagem.copy()

    # Guardar filtros para aplicar nas rechamadas depois
    filtros_para_rechamadas = filtros_adicionais

    # ---- BLOCO DE CÓDIGO CORRIGIDO E NO LUGAR CERTO ----
    # Primeiro, definimos a função de apoio para escolher o supervisor
    def supervisor_mais_frequente_tabela1(series):
        """Retorna o supervisor mais frequente no período"""
        counts = series.value_counts()
        if counts.empty:
            return 'Não Mapeado'
        return counts.index[0]  # Supervisor com mais ocorrências

    # Agora, usamos a função para pegar supervisor mais frequente
    df_agentes = df_filtrado_final.groupby(['l5_agente', 'nome_agente']).agg(
        Contagem_de_Ligacoes=('protocolo', 'count'),
        media_tempo_ligacao=('tempo_atendimento', 'mean'),
        supervisor=('supervisor', supervisor_mais_frequente_tabela1)
    ).reset_index()

    # DEBUG: Verificar qual supervisor a safe_mode escolheu para Jhennifer
    jhennifer_tabela1 = df_agentes[df_agentes['l5_agente'] == '2073']
    if not jhennifer_tabela1.empty:
        supervisor_escolhido = jhennifer_tabela1['supervisor'].iloc[0]
        nome_escolhido = jhennifer_tabela1['nome_agente'].iloc[0]
        logger.info(f"TABELA1 - L5 2073 supervisor: {supervisor_escolhido}, nome: '{nome_escolhido}'")
    else:
        logger.info("TABELA1 - L5 2073 (Jhennifer) não encontrado na Tabela 1")
    # ---- FIM DO BLOCO CORRIGIDO ----

    # SUPER DEBUG: Log detalhado das rechamadas por supervisor
    if 'supervisor' in df_para_contagem.columns:
        supervisor_counts = df_rechamadas.groupby('rechamada_atribuida_l5').size()
        logger.info(f"TABELA1 - Rechamadas por agente (rechamada_atribuida_l5): {dict(supervisor_counts)}")

        # Mapear L5 para supervisor para debug
        agent_map = df_para_contagem.drop_duplicates(subset=['l5_agente']).set_index('l5_agente')['supervisor']
        supervisor_totals = {}
        supervisor_details = {}
        for l5, count in supervisor_counts.items():
            supervisor = agent_map.get(l5, 'Desconhecido')
            supervisor_totals[supervisor] = supervisor_totals.get(supervisor, 0) + count
            if supervisor not in supervisor_details:
                supervisor_details[supervisor] = {}
            # Mapear L5 para nome do agente
            nome_agente = df_para_contagem[df_para_contagem['l5_agente'] == l5]['nome_agente'].iloc[0] if len(df_para_contagem[df_para_contagem['l5_agente'] == l5]) > 0 else 'Desconhecido'
            supervisor_details[supervisor][nome_agente] = count

        logger.info(f"TABELA1 - Total por supervisor: {supervisor_totals}")

        # LOG ESPECÍFICO PARA BEATRIZ
        if 'Beatriz Costa' in supervisor_details:
            beatriz_total = sum(supervisor_details['Beatriz Costa'].values())
            logger.info(f"TABELA1 - BEATRIZ COSTA DETALHADO:")
            logger.info(f"TABELA1 - Agentes e rechamadas: {supervisor_details['Beatriz Costa']}")
            logger.info(f"TABELA1 - Total calculado: {beatriz_total}")
            logger.info(f"TABELA1 - Total do dicionário: {supervisor_totals.get('Beatriz Costa', 0)}")

    # CORREÇÃO FINAL: Filtrar rechamadas usando MESMA LÓGICA da Tabela 2
    if filtros_para_rechamadas:
        df_rechamadas_filtradas = df_rechamadas.copy()

        # Criar mapeamento L5→supervisor baseado no período (igual à Tabela 2)
        agent_map_filtro = df_para_contagem.drop_duplicates(subset=['l5_agente']).set_index('l5_agente')['supervisor']

        # Aplicar cada filtro
        for coluna, valores in filtros_para_rechamadas.items():
            if valores:
                if coluna == 'supervisor':
                    # CORREÇÃO: Filtrar rechamadas diretamente por supervisor mapeado
                    # Aplicar o mapeamento L5→supervisor para cada rechamada e filtrar
                    def supervisor_match(row):
                        l5 = row['rechamada_atribuida_l5']
                        if pd.notna(l5):
                            supervisor = agent_map_filtro.get(l5, 'Não Mapeado')
                            return supervisor in valores
                        return False

                    antes = len(df_rechamadas_filtradas)
                    df_rechamadas_filtradas = df_rechamadas_filtradas[df_rechamadas_filtradas.apply(supervisor_match, axis=1)]
                    logger.info(f"Tabela1 - Filtro supervisor={valores}: {antes} → {len(df_rechamadas_filtradas)} rechamadas")

                elif coluna == 'nome_agente':
                    # Pegar L5s dos agentes filtrados
                    agentes_filtrados_l5 = df_para_contagem['l5_agente'].unique()
                    antes = len(df_rechamadas_filtradas)
                    df_rechamadas_filtradas = df_rechamadas_filtradas[df_rechamadas_filtradas['rechamada_atribuida_l5'].isin(agentes_filtrados_l5)]
                    logger.info(f"Tabela1 - Filtro nome_agente={valores}: {antes} → {len(df_rechamadas_filtradas)} rechamadas")
    else:
        df_rechamadas_filtradas = df_rechamadas

    contagem_rechamadas = df_rechamadas_filtradas.groupby('rechamada_atribuida_l5')['tipo_rechamada'].value_counts().unstack(fill_value=0)
    logger.info(f"Tabela1 - Contagem de rechamadas por agente: {dict(contagem_rechamadas.sum(axis=1))}")
    if 'Com Motivo' not in contagem_rechamadas: contagem_rechamadas['Com Motivo'] = 0
    if 'Sem Motivo' not in contagem_rechamadas: contagem_rechamadas['Sem Motivo'] = 0
    contagem_rechamadas = contagem_rechamadas.rename(columns={'Com Motivo': 'Rechamada com Motivo', 'Sem Motivo': 'Rechamada sem Motivo'})

    df_tabela1 = pd.merge(df_agentes, contagem_rechamadas, left_on='l5_agente', right_index=True, how='inner')
    df_tabela1[['Rechamada com Motivo', 'Rechamada sem Motivo']] = df_tabela1[['Rechamada com Motivo', 'Rechamada sem Motivo']].fillna(0).astype(int)
    df_tabela1['Rechamada Total'] = df_tabela1['Rechamada com Motivo'] + df_tabela1['Rechamada sem Motivo']
    df_tabela1['perc_com_motivo'] = (df_tabela1['Rechamada com Motivo'] / df_tabela1['Contagem_de_Ligacoes'].replace(0, pd.NA)).fillna(0)
    df_tabela1['perc_sem_motivo'] = (df_tabela1['Rechamada sem Motivo'] / df_tabela1['Contagem_de_Ligacoes'].replace(0, pd.NA)).fillna(0)
    df_tabela1['perc_total'] = (df_tabela1['Rechamada Total'] / df_tabela1['Contagem_de_Ligacoes'].replace(0, pd.NA)).fillna(0)
    df_tabela1 = df_tabela1.rename(columns={
        'nome_agente': 'Nome', 'l5_agente': 'L5', 'media_tempo_ligacao': 'Média de Ligação',
        'Contagem_de_Ligacoes': 'Contagem de Ligações', 'supervisor': 'Supervisor'
    })

    ordem_colunas_final = [
        'Nome', 'L5', 'Supervisor', 'Média de Ligação', 'Contagem de Ligações', 'Rechamada com Motivo',
        'Rechamada sem Motivo', 'Rechamada Total', 'perc_com_motivo', 'perc_sem_motivo', 'perc_total'
    ]
    # Garante que só retornará colunas que realmente existem
    df_tabela1 = df_tabela1[[col for col in ordem_colunas_final if col in df_tabela1.columns]]
    return df_tabela1

def gerar_tabela_detalhes_rechamadas(df_completo, data_inicio_filtro, data_fim_filtro, filtros_adicionais=None):
    """
    Gera tabela de detalhes de rechamadas, com suporte a filtros adicionais.
    """
    colunas_finais = [
        'Nome', 'Supervisor', 'Origem', 'DDD', 'Local (Cidade e Estado)', 'MVNO', 'Categoria',
        'Tipo de Rechamada', 'Semana', 'Tempo de Atendimento', 'Data/Hora da Chamada Original', 
        'Data/Hora da Rechamada', 'Status'
    ]
    
    if df_completo is None or df_completo.empty:
        return pd.DataFrame(columns=colunas_finais)
    
    logger.info(f"Processamento ULTRA LEVE para período {data_inicio_filtro} a {data_fim_filtro}")
    
    try:
        # ABORDAGEM MINIMALISTA: Apenas filtra e retorna dados básicos
        df_completo['data_hora_contato'] = pd.to_datetime(df_completo['data_hora_contato'])
        
        # CORRIGIDO: Usar filtro datetime igual ao da Tabela 1
        start_dt = datetime.combine(data_inicio_filtro, datetime.min.time())
        end_dt = datetime.combine(data_fim_filtro, datetime.max.time())

        rechamadas_filtro = (
            (df_completo['is_rechamada'] == True) &
            (df_completo['data_hora_contato'] >= start_dt) &
            (df_completo['data_hora_contato'] <= end_dt)
        )
        
        # Retorna todos os dados das rechamadas, sem limitação artificial
        df_rechamadas = df_completo.loc[rechamadas_filtro].copy()
        
        # IMPORTANTE: Cria campos de atribuição ANTES dos filtros
        if 'rechamada_atribuida_nome' not in df_rechamadas.columns:
            # CORREÇÃO DEFINITIVA: Usar EXATAMENTE a mesma lógica da Tabela 1
            # Aplicar safe_mode para escolher o supervisor correto quando há múltiplos
            def safe_mode(series):
                mode_result = series.mode()
                if mode_result.empty:
                    return series.iloc[0] if not series.empty else 'Não Mapeado'
                return mode_result.iloc[0]

            # Usar groupby + safe_mode igual à Tabela 1
            # CORREÇÃO: Para nome, usar o primeiro registro (dataset principal)
            # Para supervisor, usar o mais frequente (moda)
            def supervisor_mais_frequente(series):
                """Retorna o supervisor mais frequente no período"""
                counts = series.value_counts()
                if counts.empty:
                    return 'Não Mapeado'
                return counts.index[0]  # Supervisor com mais ocorrências

            agent_mapping = df_completo.groupby('l5_agente').agg(
                nome_agente=('nome_agente', 'first'),  # Nome do dataset principal
                supervisor=('supervisor', supervisor_mais_frequente)  # Supervisor mais frequente
            )

            name_map = agent_mapping['nome_agente']
            supervisor_map = agent_mapping['supervisor']

            df_rechamadas['rechamada_atribuida_nome'] = df_rechamadas['rechamada_atribuida_l5'].map(name_map).fillna('Agente Desconhecido')
            df_rechamadas['rechamada_atribuida_supervisor'] = df_rechamadas['rechamada_atribuida_l5'].map(supervisor_map).fillna('Não Mapeado')

            # DEBUG: Verificar nome exato da Jhennifer
            jhennifer_nome_tabela2 = name_map.get('2073', 'Não encontrado')
            logger.info(f"TABELA2 - Nome exato L5 2073: '{jhennifer_nome_tabela2}'")

            # DEBUG: Verificar especificamente o L5 2073 (Jhennifer)
            jhennifer_l5 = '2073'
            jhennifer_supervisor = supervisor_map.get(jhennifer_l5, 'Não Mapeado')
            jhennifer_nome = name_map.get(jhennifer_l5, 'Não Mapeado')
            jhennifer_rechamadas = len(df_rechamadas[df_rechamadas['rechamada_atribuida_l5'] == jhennifer_l5])

            logger.info(f"TABELA2 - L5 2073 (Jhennifer): Supervisor={jhennifer_supervisor}, Nome={jhennifer_nome}, Rechamadas={jhennifer_rechamadas}")

            # DEBUG: Verificar se existem rechamadas com L5 2073 no dataset completo
            total_rechamadas_2073 = len(df_completo[df_completo['rechamada_atribuida_l5'] == '2073'])
            rechamadas_2073_periodo = len(df_rechamadas[df_rechamadas['rechamada_atribuida_l5'] == '2073'])
            logger.info(f"TABELA2 - Total rechamadas L5 2073 no dataset completo: {total_rechamadas_2073}")
            logger.info(f"TABELA2 - Rechamadas L5 2073 no período filtrado: {rechamadas_2073_periodo}")

            # DEBUG: Verificar algumas rechamadas do L5 2073 se existirem
            if total_rechamadas_2073 > 0:
                amostras_2073 = df_completo[df_completo['rechamada_atribuida_l5'] == '2073'][['data_hora_contato', 'rechamada_atribuida_l5', 'is_rechamada']].head(3)
                logger.info(f"TABELA2 - Amostras rechamadas L5 2073: {amostras_2073.to_dict('records')}")

            # DEBUG: Verificar se o mapeamento está igual ao da Tabela 1
            beatriz_l5s_tabela1 = ['2000', '2034', '2092', '2107', '2105', '2156', '2158', '2073', '2039', '2059', '2075', '2116', '2028', '8011', '2033']
            beatriz_mapeados = 0
            for l5 in beatriz_l5s_tabela1:
                supervisor_mapeado = supervisor_map.get(l5, 'Não Mapeado')
                if supervisor_mapeado == 'Beatriz Costa':
                    beatriz_mapeados += 1
                logger.info(f"TABELA2 - L5 {l5} mapeado para: {supervisor_mapeado}")
            logger.info(f"TABELA2 - Total L5s da Beatriz mapeados corretamente: {beatriz_mapeados}/15")

            # DEBUG: Contar rechamadas por supervisor na Tabela 2
            rechamadas_por_supervisor = df_rechamadas['rechamada_atribuida_supervisor'].value_counts()
            logger.info(f"TABELA2 - Rechamadas por supervisor: {rechamadas_por_supervisor.to_dict()}")

            beatriz_rechamadas_tabela2 = len(df_rechamadas[df_rechamadas['rechamada_atribuida_supervisor'] == 'Beatriz Costa'])
            logger.info(f"TABELA2 - Total rechamadas Beatriz Costa: {beatriz_rechamadas_tabela2}")

            # DEBUG: Log para verificar mapeamento
            logger.info(f"Tabela2 - Registros antes do mapeamento: {len(df_rechamadas)}")
            logger.info(f"Tabela2 - Registros com supervisor mapeado: {len(df_rechamadas[df_rechamadas['rechamada_atribuida_supervisor'] != 'Não Mapeado'])}")
            logger.info(f"Tabela2 - Valores únicos de rechamada_atribuida_supervisor: {df_rechamadas['rechamada_atribuida_supervisor'].value_counts().to_dict()}")
            logger.info(f"Tabela2 - Valores únicos de rechamada_atribuida_nome: {df_rechamadas['rechamada_atribuida_nome'].value_counts().head(10).to_dict()}")

            # DEBUG ESPECÍFICO: Procurar Jessica
            jessica_rechamadas = df_rechamadas[df_rechamadas['rechamada_atribuida_nome'].str.contains('Jessica', case=False, na=False)]
            logger.info(f"Tabela2 - Rechamadas da Jessica: {len(jessica_rechamadas)}")
            if len(jessica_rechamadas) > 0:
                logger.info(f"Tabela2 - L5s da Jessica: {jessica_rechamadas['rechamada_atribuida_l5'].unique()}")

            # DEBUG ESPECÍFICO: Contar Beatriz Costa
            beatriz_rechamadas = df_rechamadas[df_rechamadas['rechamada_atribuida_supervisor'] == 'Beatriz Costa']
            logger.info(f"TABELA2 - BEATRIZ COSTA: {len(beatriz_rechamadas)} rechamadas")
            if len(beatriz_rechamadas) > 0:
                beatriz_agentes = beatriz_rechamadas.groupby('rechamada_atribuida_nome').size().to_dict()
                logger.info(f"TABELA2 - Agentes da Beatriz: {beatriz_agentes}")
                logger.info(f"TABELA2 - Total calculado: {sum(beatriz_agentes.values())}")

                # DEBUG: Verificar L5s que não foram mapeados
                l5s_esperados = ['2000', '2034', '2092', '2107', '2105', '2156', '2158', '2073', '2039', '2059', '2075', '2116', '2028', '8011', '2033']
                l5s_encontrados = beatriz_rechamadas['rechamada_atribuida_l5'].unique()
                l5s_faltando = [l5 for l5 in l5s_esperados if l5 not in l5s_encontrados]
                logger.info(f"TABELA2 - L5s esperados: {l5s_esperados}")
                logger.info(f"TABELA2 - L5s encontrados: {sorted(l5s_encontrados)}")
                logger.info(f"TABELA2 - L5s faltando: {l5s_faltando}")

                # DEBUG: Verificar se existem esses L5s no dataset completo
                for l5_faltando in l5s_faltando:
                    existe_no_dataset = len(df_rechamadas[df_rechamadas['rechamada_atribuida_l5'] == l5_faltando])
                    logger.info(f"TABELA2 - L5 {l5_faltando} existe no dataset: {existe_no_dataset} rechamadas")

                    if existe_no_dataset > 0:
                        # Verificar qual supervisor está mapeado para esse L5
                        supervisor_mapeado = df_rechamadas[df_rechamadas['rechamada_atribuida_l5'] == l5_faltando]['rechamada_atribuida_supervisor'].iloc[0]
                        nome_mapeado = df_rechamadas[df_rechamadas['rechamada_atribuida_l5'] == l5_faltando]['rechamada_atribuida_nome'].iloc[0]
                        logger.info(f"TABELA2 - L5 {l5_faltando} mapeado como: {nome_mapeado} | Supervisor: {supervisor_mapeado}")

                # DEBUG: Verificar o mapeamento original no dataset completo
                logger.info("TABELA2 - Verificando mapeamento original...")
                for l5_esperado in l5s_esperados:
                    supervisor_original = df_completo[df_completo['l5_agente'] == l5_esperado]['supervisor'].unique()
                    nome_original = df_completo[df_completo['l5_agente'] == l5_esperado]['nome_agente'].unique()
                    logger.info(f"TABELA2 - L5 {l5_esperado} original: {nome_original} | Supervisor original: {supervisor_original}")

        # Aplica filtros adicionais se fornecidos
        if filtros_adicionais:
            logger.info(f"Tabela2 - Aplicando filtros adicionais: {filtros_adicionais}")
            logger.info(f"Tabela2 - Dataset antes dos filtros: {len(df_rechamadas)} registros")
            
            for coluna, valores in filtros_adicionais.items():
                if valores:  # Só processa se valores não estão vazios
                    # MAPEAMENTO ESPECIAL: para tabela 2, nome_agente deve filtrar por rechamada_atribuida_nome
                    if coluna == 'nome_agente':
                        if 'rechamada_atribuida_nome' in df_rechamadas.columns:
                            antes = len(df_rechamadas)
                            df_rechamadas = df_rechamadas[df_rechamadas['rechamada_atribuida_nome'].isin(valores)]
                            logger.info(f"Tabela2 - Filtro {coluna}→rechamada_atribuida_nome={valores}: {antes} → {len(df_rechamadas)} registros")
                        else:
                            logger.warning(f"Tabela2 - Campo rechamada_atribuida_nome não existe ainda")
                    elif coluna == 'supervisor':
                        if 'rechamada_atribuida_supervisor' in df_rechamadas.columns:
                            antes = len(df_rechamadas)
                            df_rechamadas = df_rechamadas[df_rechamadas['rechamada_atribuida_supervisor'].isin(valores)]
                            logger.info(f"Tabela2 - Filtro {coluna}→rechamada_atribuida_supervisor={valores}: {antes} → {len(df_rechamadas)} registros")
                        else:
                            logger.warning(f"Tabela2 - Campo rechamada_atribuida_supervisor não existe ainda")
                    elif coluna in df_rechamadas.columns:
                        antes = len(df_rechamadas)
                        df_rechamadas = df_rechamadas[df_rechamadas[coluna].isin(valores)]
                        logger.info(f"Tabela2 - Filtro {coluna}={valores}: {antes} → {len(df_rechamadas)} registros")
                    else:
                        logger.warning(f"Tabela2 - Filtro ignorado {coluna} (coluna não existe)")
        
        logger.info(f"Retornando {len(df_rechamadas)} rechamadas após filtros")
        
        if df_rechamadas.empty:
            return pd.DataFrame(columns=colunas_finais)
        
        # Os campos de atribuição já foram criados antes dos filtros

        # Cria resultado usando os dados de QUEM CAUSOU a rechamada, não de quem atendeu
        df_resultado = df_rechamadas.rename(columns={
            'rechamada_atribuida_nome': 'Nome',  # MUDANÇA: usa quem causou a rechamada
            'rechamada_atribuida_supervisor': 'Supervisor',  # MUDANÇA: supervisor de quem causou
            'origem': 'Origem',
            'ddd': 'DDD',
            'local': 'Local (Cidade e Estado)',
            'mvno': 'MVNO',
            'motivo_categoria': 'Categoria',
            'tipo_rechamada': 'Tipo de Rechamada',
            'status_ligacao': 'Status'
        }).copy()
        
        # Adiciona colunas calculadas essenciais
        # IMPORTANTE: Usa tempo_atendimento_original (da chamada que causou), não da rechamada
        df_resultado['Tempo de Atendimento'] = df_resultado.get('tempo_atendimento_original', 0).apply(formatar_segundos_para_hhmmss)
        df_resultado['Semana'] = df_resultado['data_hora_contato'].apply(get_semana_customizada)
        df_resultado['Data/Hora da Chamada Original'] = df_resultado.get('data_chamada_original', '')
        df_resultado['Data/Hora da Rechamada'] = df_resultado['data_hora_contato']
        
        # Retorna apenas colunas que existem
        colunas_existentes = [col for col in colunas_finais if col in df_resultado.columns]
        resultado_final = df_resultado[colunas_existentes]
        
        logger.info(f"Tabela2 simplificada processada: {len(resultado_final)} registros")
        return resultado_final
            
    except Exception as e:
        logger.error(f"Erro no processamento simplificado da Tabela2: {e}")
        return pd.DataFrame(columns=colunas_finais)

# Função otimizar_tipos_dados removida - não é mais utilizada
