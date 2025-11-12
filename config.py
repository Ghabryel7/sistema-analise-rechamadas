# config.py - Configurações do Sistema
# VERSÃO PORTFOLIO - Dados sensíveis removidos

PASTA_PLANILHAS = "dados_exemplo"
PASTA_HISTORICO_CSV = "dados_exemplo"
PASTA_MOPS_HISTORICOS = "dados_exemplo"

# Configurações de processamento
USE_SIMPLE_SUPERVISOR_MAPPING = True
ARQUIVO_DADOS_CONSOLIDADO = "dados_consolidado.parquet"
ARQUIVO_MAPEAMENTO_TEMPORAL = f"{PASTA_MOPS_HISTORICOS}/mapeamento_supervisor.parquet"

# API - Credenciais de exemplo (não funcionais)
URL_BASE = "https://api.exemplo.com"
LOGIN_API = "usuario@exemplo.com"
SENHA_API = "senha_exemplo_123"
PRODUTO_API = "sistema"
DISPOSITIVO_API = "desktop"

# Filtros de extração
MEUS_FILTER_GROUPS = []

# MVNOs válidas para análise
GRUPOS_PARA_FILTRAR_PYTHON = [
    "OperadoraA", "OperadoraB", "OperadoraC", "OperadoraD",
    "OperadoraE", "OperadoraF", "OperadoraG", "OperadoraH"
]

# Filas para excluir da análise
FILAS_PARA_EXCLUIR = [
    "Teste", "Admin", "Sistema", "Backup"
]

# Colunas chave para identificação de duplicatas
COLUNAS_CHAVE_DUPLICATAS = ['data_hora_contato', 'protocolo', 'origem']

# Colunas para conversão de tipo texto
COLUNAS_PARA_TEXTO = ['origem', 'protocolo', 'agente', 'rechamada_atribuida']

# Chave secreta para Flask (exemplo)
SECRET_KEY = 'chave_secreta_exemplo_nao_usar_em_producao'
