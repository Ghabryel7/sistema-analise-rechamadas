#!/usr/bin/env python3
"""
Script para gerar dados mockados para demonstração do sistema
Cria dados fictícios para portfolio
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def gerar_dados_demo():
    """Gera dados mockados para demonstração"""

    np.random.seed(42)
    random.seed(42)

    # Parâmetros
    n_registros = 5000
    data_inicio = datetime(2025, 1, 1)
    data_fim = datetime.now()

    # Listas de dados mock
    operadoras = ["OperadoraA", "OperadoraB", "OperadoraC", "OperadoraD",
                  "OperadoraE", "OperadoraF", "OperadoraG", "OperadoraH"]

    agentes = [f"AGT{i:03d}" for i in range(1, 51)]
    nomes = [f"Atendente {i}" for i in range(1, 51)]
    supervisores = [f"Supervisor {i}" for i in range(1, 11)]

    motivos = ["SUPORTE TÉCNICO", "CANCELAMENTO", "INTERNET", "LIGAÇÕES",
               "PLANO / SALDO / RECARGA", "PORTABILIDADE", "RECLAMAÇÃO", "OUTRO"]

    status = ["Atendida", "Abandonada", "Transferida"]

    # Gerar dados
    dados = []

    for i in range(n_registros):
        # Data aleatória no período
        dias_diff = (data_fim - data_inicio).days
        data_contato = data_inicio + timedelta(days=random.randint(0, dias_diff),
                                               hours=random.randint(8, 18),
                                               minutes=random.randint(0, 59))

        # Dados do registro
        registro = {
            'data_hora_contato': data_contato,
            'protocolo': f"PROT{i+1:06d}",
            'origem': f"{random.randint(11, 99)}{random.randint(900000000, 999999999)}",
            'mvno': random.choice(operadoras),
            'l5_agente': random.choice(agentes),
            'nome_agente': random.choice(nomes),
            'supervisor': random.choice(supervisores),
            'tempo_atendimento': random.randint(60, 1200),
            'tempo_espera': random.randint(0, 300),
            'tempo_ligacao_total': random.randint(100, 1500),
            'status_ligacao': random.choice(status),
            'motivo_categoria': random.choice(motivos),
            'motivo_original': random.choice(motivos),
            'transferred': random.choice(['-', 'Sim']),
            'ddd': str(random.randint(11, 99)),
            'local': f"Cidade {random.randint(1, 100)} - Estado",
            'is_rechamada': random.random() < 0.15,  # 15% são rechamadas
            'causou_rechamada': False,
            'mes': data_contato.month,
            'semana': data_contato.isocalendar()[1],
            'is_expurgado': False,
        }

        dados.append(registro)

    # Criar DataFrame
    df = pd.DataFrame(dados)

    # Ajustar causou_rechamada baseado em is_rechamada
    rechamadas_ids = df[df['is_rechamada'] == True].index
    for idx in rechamadas_ids:
        if idx > 0:  # Tem registro anterior
            # Marcar registro anterior como causador
            df.at[idx-1, 'causou_rechamada'] = True
            # Adicionar dados de rastreio
            df.at[idx, 'data_hora_rechamada_original'] = df.at[idx-1, 'data_hora_contato']
            df.at[idx, 'protocolo_chamada_original'] = df.at[idx-1, 'protocolo']
            df.at[idx, 'tempo_atendimento_original'] = df.at[idx-1, 'tempo_atendimento']
            df.at[idx, 'rechamada_atribuida_l5'] = df.at[idx-1, 'l5_agente']

            # Tipo de rechamada
            dias_diff = (df.at[idx, 'data_hora_contato'] - df.at[idx-1, 'data_hora_contato']).days
            if dias_diff == 0:
                df.at[idx, 'tipo_rechamada'] = 'Mesmo Dia'
            elif dias_diff <= 3:
                df.at[idx, 'tipo_rechamada'] = 'Até 3 Dias'
            else:
                df.at[idx, 'tipo_rechamada'] = 'Mais de 3 Dias'

    # Preencher valores vazios
    df['tipo_rechamada'] = df['tipo_rechamada'].fillna('N/A')
    df['data_hora_rechamada_original'] = df['data_hora_rechamada_original'].fillna(pd.NaT)
    df['protocolo_chamada_original'] = df['protocolo_chamada_original'].fillna('-')
    df['tempo_atendimento_original'] = df['tempo_atendimento_original'].fillna(0)
    df['rechamada_atribuida_l5'] = df['rechamada_atribuida_l5'].fillna('-')

    # Adicionar coluna de data da rechamada seguinte
    df['data_da_rechamada_seguinte'] = pd.NaT
    causadores_ids = df[df['causou_rechamada'] == True].index
    for idx in causadores_ids:
        if idx < len(df) - 1:
            df.at[idx, 'data_da_rechamada_seguinte'] = df.at[idx+1, 'data_hora_contato']

    print(f"✓ Gerados {len(df)} registros mockados")
    print(f"  - Rechamadas: {df['is_rechamada'].sum()} ({df['is_rechamada'].sum()/len(df)*100:.1f}%)")
    print(f"  - Causadores: {df['causou_rechamada'].sum()}")
    print(f"  - Período: {df['data_hora_contato'].min().date()} até {df['data_hora_contato'].max().date()}")
    print(f"  - Operadoras: {df['mvno'].nunique()}")
    print(f"  - Atendentes: {df['l5_agente'].nunique()}")

    return df

if __name__ == "__main__":
    print("\n" + "="*60)
    print("GERADOR DE DADOS MOCKADOS - PORTFOLIO")
    print("="*60 + "\n")

    df = gerar_dados_demo()

    # Salvar em parquet
    output_file = "dados_consolidado.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\n✓ Dados salvos em: {output_file}")

    # Estatísticas
    print(f"\n" + "="*60)
    print("ESTATÍSTICAS DOS DADOS GERADOS")
    print("="*60)
    print(f"\nDistribuição por Operadora:")
    print(df['mvno'].value_counts())
    print(f"\nDistribuição por Mês:")
    print(df['mes'].value_counts().sort_index())

    print(f"\n" + "="*60)
    print("✓ CONCLUÍDO!")
    print("="*60 + "\n")
