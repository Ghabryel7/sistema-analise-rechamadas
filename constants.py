# constants.py
# Arquivo com listas e dicionários de mapeamento
# VERSÃO PORTFOLIO - Nomes genéricos

MVNOS_VALIDAS = [
    "OperadoraA", "OperadoraB", "OperadoraC", "OperadoraD",
    "OperadoraE", "OperadoraF", "OperadoraG", "OperadoraH"
]

PREFIXOS_MVNO_MAP = {
    # Mapeamentos com prefixo
    "OpA_": "OperadoraA", "OpB_": "OperadoraB", "OpC_": "OperadoraC",
    "OpD_": "OperadoraD", "OpE_": "OperadoraE", "OpF_": "OperadoraF",
    "OpG_": "OperadoraG", "OpH_": "OperadoraH",

    # Mapeamentos diretos
    "OperadoraA": "OperadoraA", "OperadoraB": "OperadoraB",
    "OperadoraC": "OperadoraC", "OperadoraD": "OperadoraD",
    "OperadoraE": "OperadoraE", "OperadoraF": "OperadoraF",
    "OperadoraG": "OperadoraG", "OperadoraH": "OperadoraH",

    # Filas específicas de exemplo
    "OpA_Suporte": "OperadoraA", "OpA_Cancelamento": "OperadoraA",
    "OpB_Suporte": "OperadoraB", "OpB_Cancelamento": "OperadoraB",
}

# Mapeamento de motivos de rechamada
MAPEAMENTO_MOTIVOS = {
    "Suporte": "SUPORTE TÉCNICO",
    "Cancelamento": "CANCELAMENTO",
    "Internet": "INTERNET",
    "Ligacoes": "LIGAÇÕES",
    "Plano": "PLANO / SALDO / RECARGA",
    "Portabilidade": "PORTABILIDADE",
    "Reclamacao": "RECLAMAÇÃO",
    "Outro": "OUTRO",
}
