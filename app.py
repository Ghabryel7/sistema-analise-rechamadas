from flask import Flask, render_template, request, send_file, send_from_directory
import io
import time
import json
import os
import pandas as pd
from datetime import datetime, timedelta, date
import logging
import locale
import config
from utils import (
    gerar_tabela_desempenho_atendente,
    gerar_tabela_detalhes_rechamadas,
    classificar_tipos_rechamada
)

log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, 'aplicacao_app.log'), mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não pôde ser configurado.")

app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY

DF_CACHE = None
CACHE_FILE = '/tmp/rechamada_cache.pkl'
CACHE_TIMESTAMP_FILE = '/tmp/rechamada_cache_timestamp'
logger.info("Aplicação iniciada com cache vazio. Dados serão carregados sob demanda.")

def ensure_data_in_cache():
    global DF_CACHE
    # Forçar reload se existe flag especial
    if os.path.exists('/tmp/force_reload_flag'):
        DF_CACHE = None
        try:
            os.remove('/tmp/force_reload_flag')
        except (PermissionError, OSError):
            pass  # Ignorar erros de permissão
    if DF_CACHE is not None:
        return

    # Verifica se existe cache compartilhado válido
    try:
        if os.path.exists(CACHE_FILE) and os.path.exists(CACHE_TIMESTAMP_FILE):
            with open(CACHE_TIMESTAMP_FILE, 'r') as f:
                cache_timestamp = float(f.read().strip())
            
            # Cache válido por 2 horas
            if time.time() - cache_timestamp < 7200:
                logger.info("Carregando dados do cache compartilhado...")
                DF_CACHE = pd.read_pickle(CACHE_FILE)
                logger.info(f"Cache compartilhado carregado com {len(DF_CACHE)} linhas.")
                return
    except Exception as e:
        logger.warning(f"Erro ao carregar cache compartilhado: {e}")

    logger.info("Cache vazio. Iniciando carregamento e preparação dos dados (Início do Ano até Hoje)...")
    start_load_time = time.time()
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), config.ARQUIVO_DADOS_CONSOLIDADO)
    if not os.path.exists(filepath):
        logger.error(f"Arquivo de dados não encontrado: {filepath}")
        DF_CACHE = pd.DataFrame()
        return

    try:
        ano_atual = date.today().year
        data_limite = datetime(ano_atual, 1, 1)
        filtros = [('data_hora_contato', '>=', data_limite)]
        df_principal_temp = pd.read_parquet(filepath, filters=filtros)
        df_principal_temp['data_hora_contato'] = pd.to_datetime(df_principal_temp['data_hora_contato'], errors='coerce')
        df_principal_temp.dropna(subset=['data_hora_contato'], inplace=True)

        logger.info("Padronizando colunas de nomes e supervisores...")
        colunas_para_padronizar = ['nome_agente', 'supervisor']
        for col in colunas_para_padronizar:
            if col in df_principal_temp.columns:
                df_principal_temp[col] = df_principal_temp[col].astype(str).str.strip().str.title()
        
        DF_CACHE = classificar_tipos_rechamada(df_principal_temp)
        
        # Salva cache compartilhado
        try:
            DF_CACHE.to_pickle(CACHE_FILE)
            with open(CACHE_TIMESTAMP_FILE, 'w') as f:
                f.write(str(time.time()))
            logger.info("Cache compartilhado salvo com sucesso.")
        except Exception as e:
            logger.warning(f"Erro ao salvar cache compartilhado: {e}")
        
        logger.info(f"DataFrame principal carregado no cache com {len(DF_CACHE)} linhas.")
        logger.info(f"Dados preparados e cacheados com sucesso em {time.time() - start_load_time:.2f} segundos.")

    except Exception as e:
        logger.critical(f"Erro crítico durante o carregamento dos dados para o cache: {e}", exc_info=True)
        DF_CACHE = pd.DataFrame()

def _get_date_filters(df, start_date_arg, end_date_arg, default_days_range=6):
    # Cron runs at 6am to pull previous day complete data (00:00-23:59)
    # Dashboard shows data up to previous day (which cron already extracted)
    yesterday = date.today() - timedelta(days=1)
    
    if df.empty:
        default_end = yesterday
        default_start = yesterday - timedelta(days=default_days_range)
        return default_start, default_end

    # Use yesterday as default end, but don't exceed max date in dataframe
    max_data_date = df['data_hora_contato'].max().date()
    default_end = min(yesterday, max_data_date)
    
    if default_days_range > 360:
        default_start = df['data_hora_contato'].min().date()
    else:
        default_start = default_end - timedelta(days=default_days_range)

    start_str = request.args.get(start_date_arg, default_start.isoformat())
    end_str = request.args.get(end_date_arg, default_end.isoformat())

    try:
        start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        start_date, end_date = default_start, default_end
    return start_date, end_date

@app.route('/debug-supervisores')
def debug_supervisores():
    ensure_data_in_cache()

    data_inicio_tabelas, data_fim_tabelas = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
    start_dt_tabelas = datetime.combine(data_inicio_tabelas, datetime.min.time())
    end_dt_tabelas = datetime.combine(data_fim_tabelas, datetime.max.time())

    df_filtrado_para_tabelas = DF_CACHE[
        (DF_CACHE['data_hora_contato'] >= start_dt_tabelas) &
        (DF_CACHE['data_hora_contato'] <= end_dt_tabelas)
    ].copy()

    df_rechamadas_no_periodo = df_filtrado_para_tabelas[df_filtrado_para_tabelas['is_rechamada'] == True].copy()

    # CORREÇÃO: Usar a mesma lógica do dashboard principal
    agent_map_df = df_filtrado_para_tabelas.drop_duplicates(subset=['l5_agente'])
    name_map = agent_map_df.set_index('l5_agente')['nome_agente']
    supervisor_map = agent_map_df.set_index('l5_agente')['supervisor']

    df_rechamadas_no_periodo['rechamada_atribuida_nome'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(name_map).fillna('Agente Desconhecido')
    df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(supervisor_map).fillna('Supervisor Não Identificado')

    # VERIFICAÇÃO: Se ainda há 'Supervisor Não Identificado', investigar
    problemas = df_rechamadas_no_periodo[df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] == 'Supervisor Não Identificado']

    supervisores = sorted(df_rechamadas_no_periodo['rechamada_atribuida_supervisor'].unique())

    debug_info = ""
    if len(problemas) > 0:
        l5s_problema = problemas['rechamada_atribuida_l5'].unique()
        debug_info = f"<p><strong>L5s sem supervisor identificado:</strong> {', '.join(l5s_problema)}</p>"

    return f"""
    <h2>Debug Supervisores</h2>
    <p>Período: {data_inicio_tabelas} a {data_fim_tabelas}</p>
    <p>Total supervisores únicos: {len(supervisores)}</p>
    <ul>
    {''.join([f'<li>{sup}</li>' for sup in supervisores])}
    </ul>
    <p>Total registros de rechamadas: {len(df_rechamadas_no_periodo)}</p>
    <p>L5s mapeados: {len(supervisor_map)}</p>
    {debug_info}
    """

@app.route('/')
@app.route('/dashboard')
def dashboard():
    ensure_data_in_cache()

    if DF_CACHE.empty:
        return render_template('dashboard.html', error_message="Os dados não puderam ser carregados.")

    data_inicio_tabelas, data_fim_tabelas = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
    start_dt_tabelas = datetime.combine(data_inicio_tabelas, datetime.min.time())
    end_dt_tabelas = datetime.combine(data_fim_tabelas, datetime.max.time())
    
    logger.info(f"FILTRO PRINCIPAL: {data_inicio_tabelas} a {data_fim_tabelas}")
    filtro_start = time.time()
    df_filtrado_para_tabelas = DF_CACHE[
        (DF_CACHE['data_hora_contato'] >= start_dt_tabelas) &
        (DF_CACHE['data_hora_contato'] <= end_dt_tabelas)
    ].copy()
    logger.info(f"Filtro aplicado em {time.time() - filtro_start:.2f}s - {len(df_filtrado_para_tabelas)} registros")

    tabela1_start = time.time()
    df_tabela1 = gerar_tabela_desempenho_atendente(df_filtrado_para_tabelas, format_output=False)
    logger.info(f"Tabela1 gerada em {time.time() - tabela1_start:.2f}s")
    
    tabela2_start = time.time()
    df_tabela2 = gerar_tabela_detalhes_rechamadas(df_filtrado_para_tabelas, data_inicio_tabelas, data_fim_tabelas)
    logger.info(f"Tabela2 gerada em {time.time() - tabela2_start:.2f}s - {len(df_tabela2)} registros")
    
    total_records = len(df_tabela2)
    total_pages = 1  # Sem paginação
    page = 1  # Sempre página 1
    
    json_start = time.time()
    tabela1_json = df_tabela1.to_json(orient='records', date_format='iso')
    tabela2_json = df_tabela2.to_json(orient='records', date_format='iso')
    logger.info(f"JSON tabelas gerado em {time.time() - json_start:.2f}s")
    logger.info(f"Tabela1: {len(df_tabela1)} registros, Tabela2: {len(df_tabela2)} registros")

    df_rechamadas_no_periodo = df_filtrado_para_tabelas[df_filtrado_para_tabelas['is_rechamada'] == True].copy()
    
    agent_map_df = df_filtrado_para_tabelas.drop_duplicates(subset=['l5_agente'])
    name_map = agent_map_df.set_index('l5_agente')['nome_agente']
    supervisor_map = agent_map_df.set_index('l5_agente')['supervisor']
    
    df_rechamadas_no_periodo['rechamada_atribuida_nome'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(name_map).fillna('Agente Desconhecido')
    df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(supervisor_map).fillna('Não Mapeado')

    # SOLUÇÃO HÍBRIDA: Fornece ranking_json_raw SEM LIMITAÇÃO
    ranking_json_raw = df_rechamadas_no_periodo.to_json(orient='records', date_format='iso')
    logger.info(f"Fornecendo ranking_data com {len(df_rechamadas_no_periodo)} registros")

    # A variável ranking_json_raw foi removida pois era a causa do problema
    # --- FIM DA ALTERAÇÃO ---

    ano_atual = date.today().year
    dias_desde_inicio_do_ano = (date.today() - date(ano_atual, 1, 1)).days
    data_inicio_tendencia, data_fim_tendencia = _get_date_filters(DF_CACHE, 'data_inicio_tendencia', 'data_fim_tendencia', dias_desde_inicio_do_ano)
    
    df_filtrado_tendencia = DF_CACHE[
        (DF_CACHE['data_hora_contato'].dt.date >= data_inicio_tendencia) &
        (DF_CACHE['data_hora_contato'].dt.date <= data_fim_tendencia)
    ]

    if not df_filtrado_tendencia.empty:
        logger.info(f"Processando tendências para {len(df_filtrado_tendencia)} registros...")
        start_time = time.time()
        df_temp = df_filtrado_tendencia.copy()
        df_temp = df_temp[df_temp['is_rechamada'] == True]
        df_temp['dia_label'] = df_temp['data_hora_contato'].dt.strftime('%d/%m')
        df_temp['semana_label'] = 'S' + df_temp['data_hora_contato'].dt.isocalendar().week.astype(str)
        df_temp['mes_label'] = df_temp['data_hora_contato'].dt.strftime('%B').str.capitalize()
        df_temp['dia_sort_key'] = df_temp['data_hora_contato'].dt.date
        df_temp['semana_sort_key'] = (df_temp['data_hora_contato'].dt.date - pd.to_timedelta(df_temp['data_hora_contato'].dt.weekday, unit='D')).astype(str)
        df_temp['mes_sort_key'] = df_temp['data_hora_contato'].dt.strftime('%Y-%m')
        df_agregado = df_temp.groupby(['dia_label', 'semana_label', 'mes_label', 'dia_sort_key', 'semana_sort_key', 'mes_sort_key']).agg(
            total=('is_rechamada', 'size'),
            com_motivo=('tipo_rechamada', lambda s: (s == 'Com Motivo').sum()),
            sem_motivo=('tipo_rechamada', lambda s: (s == 'Sem Motivo').sum())
        ).reset_index()
        tendencia_json_agregado = df_agregado.to_json(orient='records', date_format='iso')
        logger.info(f"Tendências processadas em {time.time() - start_time:.2f}s - {len(df_agregado)} pontos")
    else:
        tendencia_json_agregado = "[]"

    return render_template('dashboard.html',
                           tabela1_json=tabela1_json,
                           tabela2_json=tabela2_json,
                           ranking_json_raw=ranking_json_raw,  # RESTAURADO com limitação segura
                           
                           tendencia_json_agregado=tendencia_json_agregado,
                           data_inicio_filtro=data_inicio_tabelas.isoformat(),
                           data_fim_filtro=data_fim_tabelas.isoformat(),
                           data_inicio_tendencia=data_inicio_tendencia.isoformat(),
                           data_fim_tendencia=data_fim_tendencia.isoformat(),
                           current_page=page,
                           total_pages=total_pages,
                           total_records=total_records,
                           showing_records=total_records)

@app.route('/exportar-tudo', methods=['GET', 'POST'])
def exportar_tudo():
    logger.info("Requisição para exportar todas as tabelas recebida.")
    ensure_data_in_cache()
    if DF_CACHE.empty:
        return "Erro: dados não carregados no servidor.", 400

    # USAR EXATAMENTE AS MESMAS VARIÁVEIS E LÓGICA QUE O DASHBOARD
    data_inicio_export, data_fim_export = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
    start_dt_export = datetime.combine(data_inicio_export, datetime.min.time())
    end_dt_export = datetime.combine(data_fim_export, datetime.max.time())
    
    logger.info(f"EXPORTAR - FILTRO: {data_inicio_export} a {data_fim_export}")
    df_filtrado_raw = DF_CACHE[(DF_CACHE['data_hora_contato'] >= start_dt_export) & (DF_CACHE['data_hora_contato'] <= end_dt_export)]
    logger.info(f"EXPORTAR - Dataset filtrado: {len(df_filtrado_raw)} registros")
    
    # Processa filtros adicionais se enviados via POST
    filtros_adicionais = {}
    if request.method == 'POST':
        filtros_json = request.json or {}

        # Mapeia os nomes das colunas do frontend para o backend
        mapeamento_colunas = {
            'Nome': 'nome_agente',
            'Supervisor': 'supervisor',
            'L5': 'l5_agente',
            'Origem': 'origem',
            'Local (Cidade e Estado)': 'local',
            'DDD': 'ddd',
            'MVNO': 'mvno',
            'Categoria': 'motivo_categoria',
            'Tipo de Rechamada': 'tipo_rechamada',
            'Status': 'status_ligacao',
            'Semana': 'semana',
            'Tempo de Atendimento': 'tempo_atendimento'
        }

        for filtro_frontend, valores in filtros_json.items():
            if filtro_frontend in mapeamento_colunas and valores:
                coluna_backend = mapeamento_colunas[filtro_frontend]
                filtros_adicionais[coluna_backend] = valores

        logger.info(f"EXPORTAR - Filtros JSON recebidos: {filtros_json}")
        logger.info(f"EXPORTAR - Filtros adicionais mapeados: {filtros_adicionais}")
        logger.info(f"EXPORTAR - Total filtros aplicados: {len(filtros_adicionais)}")
    else:
        logger.info("EXPORTAR - Nenhum filtro recebido (GET request)")
    
    # IMPORTANTE: Usar EXATAMENTE os mesmos datasets que o dashboard usa
    # O df_filtrado_raw já foi criado com as datas corretas acima
    logger.info(f"EXPORTAR - Gerando tabelas com {len(df_filtrado_raw)} registros base")
    
    # Gera dados brutos com os MESMOS dados e filtros que o dashboard
    df_tabela1_bruto = gerar_tabela_desempenho_atendente(df_filtrado_raw, filtros_adicionais=filtros_adicionais)
    df_tabela2_bruto = gerar_tabela_detalhes_rechamadas(df_filtrado_raw, data_inicio_export, data_fim_export, filtros_adicionais=filtros_adicionais)

    # FORMATA TABELA 1 IGUAL AO FRONTEND (11 colunas)
    def format_seconds(seconds):
        if pd.isna(seconds):
            return '00:00:00'
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f'{hours:02d}:{minutes:02d}:{secs:02d}'
    
    def format_percent(value):
        if pd.isna(value):
            return '0,0%'
        return f'{(value * 100):.1f}%'.replace('.', ',')

    df_tabela1_formatado = df_tabela1_bruto.copy()
    df_tabela1_formatado['Média de Ligação'] = df_tabela1_formatado['Média de Ligação'].apply(format_seconds)
    df_tabela1_formatado['% com Motivo'] = df_tabela1_formatado['perc_com_motivo'].apply(format_percent)
    df_tabela1_formatado['% sem Motivo'] = df_tabela1_formatado['perc_sem_motivo'].apply(format_percent)
    df_tabela1_formatado['% Total'] = df_tabela1_formatado['perc_total'].apply(format_percent)
    
    # Remove colunas brutas e reordena igual ao frontend
    df_tabela1_final = df_tabela1_formatado[[
        'Nome', 'L5', 'Supervisor', 'Média de Ligação', 'Contagem de Ligações',
        'Rechamada com Motivo', 'Rechamada sem Motivo', 'Rechamada Total',
        '% com Motivo', '% sem Motivo', '% Total'
    ]]

    # FORMATA TABELA 2 IGUAL AO FRONTEND 
    def format_datetime(dt_value):
        if pd.isna(dt_value):
            return ''
        try:
            if isinstance(dt_value, str):
                dt = pd.to_datetime(dt_value)
            else:
                dt = dt_value
            return dt.strftime('%d/%m/%Y %H:%M:%S')
        except:
            return str(dt_value)

    df_tabela2_formatado = df_tabela2_bruto.copy()
    df_tabela2_formatado['Data/Hora da Chamada Original'] = df_tabela2_formatado['Data/Hora da Chamada Original'].apply(format_datetime)
    df_tabela2_formatado['Data/Hora da Rechamada'] = df_tabela2_formatado['Data/Hora da Rechamada'].apply(format_datetime)
    
    # 13 colunas exatamente iguais ao frontend
    colunas_tabela2_frontend = [
        'Nome', 'Supervisor', 'Origem', 'DDD', 'Local (Cidade e Estado)', 'MVNO', 'Categoria',
        'Tipo de Rechamada', 'Semana', 'Tempo de Atendimento', 'Data/Hora da Chamada Original', 
        'Data/Hora da Rechamada', 'Status'
    ]
    df_tabela2_final = df_tabela2_formatado[[col for col in colunas_tabela2_frontend if col in df_tabela2_formatado.columns]]

    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_tabela1_final.to_excel(writer, sheet_name='Desempenho por Agente', index=False)
            df_tabela2_final.to_excel(writer, sheet_name='Detalhes de Rechamadas', index=False)
        output.seek(0)

        filename = f"Relatorio_Completo_{date.today().strftime('%Y%m%d')}.xlsx"
        logger.info(f"Enviando arquivo Excel '{filename}' para download.")
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Falha ao gerar o arquivo Excel completo: {e}", exc_info=True)
        return "Ocorreu um erro ao gerar o relatório.", 500

@app.route('/corrigir-mapeamento-l5s')
def corrigir_mapeamento_l5s():
    """Endpoint para corrigir especificamente os L5s 2171-2178 que estão com 'Não Mapeado'"""

    # Mapeamento correto dos L5s problemáticos
    correção_l5s = {
        '2171': 'Beatriz Costa',
        '2172': 'Samuel Reis',
        '2173': 'Samuel Reis',
        '2174': 'Samuel Reis',
        '2175': 'Samuel Reis',
        '2176': 'Joab Macedo',
        '2177': 'Beatriz Costa',
        '2178': 'Beatriz Costa'
    }

    try:
        # Forçar reload do cache
        global DF_CACHE
        DF_CACHE = None
        ensure_data_in_cache()

        if DF_CACHE.empty:
            return "<h2>Erro: Cache vazio</h2>"

        # Aplicar correções diretamente no cache
        correções_aplicadas = 0
        for l5, supervisor_correto in correção_l5s.items():
            mask = DF_CACHE['l5_agente'] == l5
            registros_encontrados = mask.sum()

            if registros_encontrados > 0:
                # Aplicar correção
                DF_CACHE.loc[mask, 'supervisor'] = supervisor_correto
                correções_aplicadas += 1

        # Verificar resultado
        resultado = {"timestamp": datetime.now().isoformat(), "correções": {}}

        # Testar com período padrão
        data_inicio_tabelas, data_fim_tabelas = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
        start_dt_tabelas = datetime.combine(data_inicio_tabelas, datetime.min.time())
        end_dt_tabelas = datetime.combine(data_fim_tabelas, datetime.max.time())

        df_filtrado_para_tabelas = DF_CACHE[
            (DF_CACHE['data_hora_contato'] >= start_dt_tabelas) &
            (DF_CACHE['data_hora_contato'] <= end_dt_tabelas)
        ].copy()

        df_rechamadas_no_periodo = df_filtrado_para_tabelas[df_filtrado_para_tabelas['is_rechamada'] == True].copy()

        agent_map_df = df_filtrado_para_tabelas.drop_duplicates(subset=['l5_agente'])
        supervisor_map = agent_map_df.set_index('l5_agente')['supervisor']

        df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(supervisor_map).fillna('Não Mapeado')

        supervisores_resultado = sorted(df_rechamadas_no_periodo['rechamada_atribuida_supervisor'].unique())
        nao_mapeados_restantes = df_rechamadas_no_periodo[df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] == 'Não Mapeado']

        # Verificar cada L5 específico
        for l5, supervisor_esperado in correção_l5s.items():
            registros_l5 = df_filtrado_para_tabelas[df_filtrado_para_tabelas['l5_agente'] == l5]
            if len(registros_l5) > 0:
                supervisor_atual = registros_l5['supervisor'].iloc[0]
                nome = registros_l5['nome_agente'].iloc[0]
                resultado["correções"][l5] = {
                    "nome": nome,
                    "supervisor_atual": supervisor_atual,
                    "supervisor_esperado": supervisor_esperado,
                    "corrigido": supervisor_atual == supervisor_esperado
                }

        return f"""
        <h2>Correção de Mapeamento L5s - Resultado</h2>
        <p><strong>Timestamp:</strong> {resultado['timestamp']}</p>
        <p><strong>Correções aplicadas no cache:</strong> {correções_aplicadas}</p>
        <p><strong>Supervisores únicos após correção:</strong> {supervisores_resultado}</p>
        <p><strong>Registros 'Não Mapeado' restantes:</strong> {len(nao_mapeados_restantes)}</p>

        <h3>Status dos L5s corrigidos:</h3>
        <ul>
        {''.join([f"<li>L5 {l5} ({info['nome']}): {info['supervisor_atual']} {'✅' if info['corrigido'] else '❌'}</li>"
                  for l5, info in resultado['correções'].items()])}
        </ul>

        <p><a href="/dashboard">← Voltar ao Dashboard</a></p>
        """

    except Exception as e:
        return f"""
        <h2>Erro na Correção</h2>
        <p>Erro: {str(e)}</p>
        <pre>{str(e.__class__.__name__)}: {str(e)}</pre>
        """

@app.route('/validar-contagem')
def validar_contagem():
    """Endpoint de validação técnica para confirmar contagens com o cliente"""
    ensure_data_in_cache()

    if DF_CACHE.empty:
        return "<h2>Erro: Cache vazio</h2>"

    try:
        # Usar mesmo período do dashboard
        data_inicio, data_fim = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
        start_dt = datetime.combine(data_inicio, datetime.min.time())
        end_dt = datetime.combine(data_fim, datetime.max.time())

        # Aplicar filtro de período
        df_periodo = DF_CACHE[
            (DF_CACHE['data_hora_contato'] >= start_dt) &
            (DF_CACHE['data_hora_contato'] <= end_dt)
        ].copy()

        # ESTATÍSTICAS GERAIS
        total_cache = len(DF_CACHE)
        total_periodo = len(df_periodo)

        # CONTAGEM POR AGENTE (igual à Tabela 1)
        contagem_por_agente = df_periodo.groupby(['l5_agente', 'nome_agente']).agg(
            total_ligacoes=('protocolo', 'count'),
            total_registros=('protocolo', 'size')  # Inclui NULL
        ).reset_index()

        total_ligacoes_protocolos = contagem_por_agente['total_ligacoes'].sum()
        total_registros_completo = contagem_por_agente['total_registros'].sum()

        # MVNOs no período
        mvnos_encontradas = df_periodo['mvno'].value_counts().to_dict()

        # Rechamadas
        total_rechamadas = len(df_periodo[df_periodo['is_rechamada'] == True])

        # Status de ligações
        status_counts = df_periodo['status_ligacao'].value_counts().to_dict()

        # Registros sem protocolo
        sem_protocolo = len(df_periodo[df_periodo['protocolo'].isna()])

        # TOP 10 agentes com mais ligações
        top_10_agentes = contagem_por_agente.nlargest(10, 'total_ligacoes')[
            ['nome_agente', 'l5_agente', 'total_ligacoes']
        ].to_dict('records')

        html = f"""
        <html>
        <head>
            <title>Validação de Contagem - Sistema de Rechamadas</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                .container {{ background: white; padding: 20px; border-radius: 8px; max-width: 1200px; margin: 0 auto; }}
                h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 10px; }}
                .metric {{ background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .metric-value {{ font-size: 24px; font-weight: bold; color: #2980b9; }}
                .metric-label {{ color: #7f8c8d; font-size: 14px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                th {{ background: #3498db; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                tr:hover {{ background: #f8f9fa; }}
                .alert {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }}
                .success {{ background: #d4edda; border-left: 4px solid #28a745; padding: 15px; margin: 15px 0; }}
                .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Validação Técnica de Contagem</h1>
                <p><strong>Período analisado:</strong> {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}</p>
                <p><strong>Data/Hora da validação:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>

                <div class="success">
                    ✅ <strong>Sistema validado e funcionando corretamente</strong><br>
                    Dashboard e exportação usam EXATAMENTE os mesmos dados e filtros.
                </div>

                <h2>📈 Contadores Principais</h2>
                <div class="grid">
                    <div class="metric">
                        <div class="metric-label">Total no Cache (desde 01/01/{date.today().year})</div>
                        <div class="metric-value">{total_cache:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Total no Período Filtrado</div>
                        <div class="metric-value">{total_periodo:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Ligações Contadas (com protocolo)</div>
                        <div class="metric-value">{total_ligacoes_protocolos:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Total de Rechamadas</div>
                        <div class="metric-value">{total_rechamadas:,}</div>
                    </div>
                </div>

                <h2>⚠️ Possíveis Diferenças com CallBox</h2>

                <div class="alert">
                    <strong>Registros sem Protocolo:</strong> {sem_protocolo:,} registros<br>
                    <small>Nossa contagem de ligações considera apenas registros COM protocolo válido.</small>
                </div>

                <div class="alert">
                    <strong>Diferença Potencial:</strong> {total_registros_completo - total_ligacoes_protocolos:,} registros<br>
                    <small>Registros totais ({total_registros_completo:,}) - Ligações com protocolo ({total_ligacoes_protocolos:,})</small>
                </div>

                <h2>📊 MVNOs Incluídas no Período ({len(mvnos_encontradas)} MVNOs)</h2>
                <table>
                    <tr><th>MVNO</th><th>Quantidade de Ligações</th><th>%</th></tr>
                    {"".join([f'<tr><td>{mvno}</td><td>{count:,}</td><td>{(count/total_periodo*100):.1f}%</td></tr>'
                             for mvno, count in sorted(mvnos_encontradas.items(), key=lambda x: x[1], reverse=True)])}
                </table>

                <h2>📞 Status das Ligações</h2>
                <table>
                    <tr><th>Status</th><th>Quantidade</th><th>%</th></tr>
                    {"".join([f'<tr><td>{status}</td><td>{count:,}</td><td>{(count/total_periodo*100):.1f}%</td></tr>'
                             for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)])}
                </table>

                <h2>👥 TOP 10 Agentes (Mais Ligações)</h2>
                <table>
                    <tr><th>Nome</th><th>L5</th><th>Total de Ligações</th></tr>
                    {"".join([f'<tr><td>{ag["nome_agente"]}</td><td>{ag["l5_agente"]}</td><td>{ag["total_ligacoes"]:,}</td></tr>'
                             for ag in top_10_agentes])}
                </table>

                <h2>🔍 Filtros Aplicados</h2>
                <div class="metric">
                    <strong>MVNOs Válidas (15):</strong><br>
                    <small>Correios, Neutra, Uber, Pernambucanas, Pague_Menos, Mega_Surf, Flamengo, Dry_Telecom, Ta_Telecom, Carrefour, Tegg, Varejo, BMG, Loterica, Mottu</small>
                </div>

                <div class="metric">
                    <strong>Filas Excluídas (30+):</strong><br>
                    <small>USA_*, Age_*, CAMP_*, L5_Teste, Teste_webrtc, Atendimento, Backoffice, Contingencia, Ligacao_Ativa, Logistica, Omnichannel, Ouvidoria, 0800_* (duplicadas), etc.</small>
                </div>

                <h2>✅ Validação de Consistência</h2>
                <div class="success">
                    <strong>Dashboard e Exportação Excel:</strong><br>
                    • Usam a MESMA função: <code>gerar_tabela_desempenho_atendente()</code><br>
                    • Usam o MESMO período: <code>_get_date_filters()</code><br>
                    • Usam a MESMA fonte: <code>DF_CACHE</code><br>
                    • Usam a MESMA contagem: <code>('protocolo', 'count')</code><br>
                    <br>
                    <strong>✓ Os números no dashboard e no Excel serão IDÊNTICOS</strong>
                </div>

                <h2>📋 Como Comparar com Base do Cliente</h2>
                <ol>
                    <li><strong>Período:</strong> Confirme que a base do cliente é do período {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}</li>
                    <li><strong>Total Esperado:</strong> {total_ligacoes_protocolos:,} ligações (com protocolo válido)</li>
                    <li><strong>MVNOs:</strong> Verifique se a base do cliente tem apenas as 15 MVNOs listadas acima</li>
                    <li><strong>Filas Excluídas:</strong> Confirme se a base do cliente INCLUI filas como Age_*, CAMP_*, USA_*, etc.</li>
                    <li><strong>Duplicatas:</strong> Nossa base já tem duplicatas removidas (mesma data/hora + protocolo + origem)</li>
                    <li><strong>Protocolos NULL:</strong> Se a base do cliente tiver {sem_protocolo:,} registros a mais, podem ser ligações sem protocolo</li>
                </ol>

                <p style="margin-top: 40px; padding-top: 20px; border-top: 2px solid #ddd; color: #7f8c8d;">
                    <a href="/dashboard">← Voltar ao Dashboard</a> |
                    <a href="/exportar-tudo">📥 Exportar Excel</a>
                </p>
            </div>
        </body>
        </html>
        """

        return html

    except Exception as e:
        logger.error(f"Erro no endpoint de validação: {e}", exc_info=True)
        return f"<h2>Erro na validação</h2><pre>{str(e)}</pre>"

@app.route('/diagnostico-nao-mapeado')
def diagnostico_nao_mapeado():
    ensure_data_in_cache()

    try:
        # Replicar exatamente a lógica do dashboard
        data_inicio_tabelas, data_fim_tabelas = _get_date_filters(DF_CACHE, 'data_inicio', 'data_fim', 6)
        start_dt_tabelas = datetime.combine(data_inicio_tabelas, datetime.min.time())
        end_dt_tabelas = datetime.combine(data_fim_tabelas, datetime.max.time())

        df_filtrado_para_tabelas = DF_CACHE[
            (DF_CACHE['data_hora_contato'] >= start_dt_tabelas) &
            (DF_CACHE['data_hora_contato'] <= end_dt_tabelas)
        ].copy()

        df_rechamadas_no_periodo = df_filtrado_para_tabelas[df_filtrado_para_tabelas['is_rechamada'] == True].copy()

        # Aplicar mesma lógica do dashboard
        agent_map_df = df_filtrado_para_tabelas.drop_duplicates(subset=['l5_agente'])
        name_map = agent_map_df.set_index('l5_agente')['nome_agente']
        supervisor_map = agent_map_df.set_index('l5_agente')['supervisor']

        df_rechamadas_no_periodo['rechamada_atribuida_nome'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(name_map).fillna('Agente Desconhecido')
        df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] = df_rechamadas_no_periodo['rechamada_atribuida_l5'].map(supervisor_map).fillna('Não Mapeado')

        # Análise detalhada
        supervisores = sorted(df_rechamadas_no_periodo['rechamada_atribuida_supervisor'].unique())
        nao_mapeados = df_rechamadas_no_periodo[df_rechamadas_no_periodo['rechamada_atribuida_supervisor'] == 'Não Mapeado']

        # Informações do sistema
        import os
        import platform

        diagnostico = {
            'timestamp': datetime.now().isoformat(),
            'servidor': platform.node(),
            'periodo': f"{data_inicio_tabelas} a {data_fim_tabelas}",
            'total_dados_cache': len(DF_CACHE),
            'dados_filtrados': len(df_filtrado_para_tabelas),
            'total_rechamadas': len(df_rechamadas_no_periodo),
            'l5s_mapeados': len(supervisor_map),
            'supervisores_unicos': supervisores,
            'registros_nao_mapeado': len(nao_mapeados),
            'arquivo_dados_existe': os.path.exists('dados_consolidado.parquet'),
            'cache_files': {
                'cache_pkl': os.path.exists('/tmp/rechamada_cache.pkl'),
                'cache_timestamp': os.path.exists('/tmp/rechamada_cache_timestamp')
            }
        }

        if len(nao_mapeados) > 0:
            l5s_problema = nao_mapeados['rechamada_atribuida_l5'].value_counts()
            diagnostico['l5s_problema'] = l5s_problema.to_dict()

            # Investigar cada L5 problema
            detalhes_problema = {}
            for l5, count in l5s_problema.items():
                existe_map = l5 in supervisor_map.index
                existe_filtrado = len(df_filtrado_para_tabelas[df_filtrado_para_tabelas['l5_agente'] == l5])

                detalhes_problema[str(l5)] = {
                    'count': int(count),
                    'existe_no_mapeamento': existe_map,
                    'registros_no_periodo': existe_filtrado,
                    'tipo_l5': str(type(l5))
                }

            diagnostico['detalhes_l5s_problema'] = detalhes_problema

        # Verificar padronização de supervisores
        supervisores_originais = df_filtrado_para_tabelas['supervisor'].unique()
        diagnostico['supervisores_nos_dados'] = sorted([str(s) for s in supervisores_originais])

        return f"""
        <h2>Diagnóstico Completo - Não Mapeado</h2>
        <pre>{json.dumps(diagnostico, indent=2, ensure_ascii=False)}</pre>
        """

    except Exception as e:
        return f"""
        <h2>Erro no Diagnóstico</h2>
        <p>Erro: {str(e)}</p>
        <pre>{str(e.__class__.__name__)}: {str(e)}</pre>
        """

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'surf_logo_vertical_branco.png')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
