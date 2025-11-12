# 📊 Sistema de Análise de Rechamadas

> Sistema inteligente para análise e monitoramento de rechamadas em call centers, desenvolvido com Python/Flask e processamento otimizado de dados.

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-3.0+-green.svg)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-orange.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

</div>

---

## 🎯 Sobre o Projeto

Sistema web desenvolvido para análise de rechamadas de atendimento em call centers. A aplicação extrai dados via API, processa informações de múltiplas operadoras e gera relatórios detalhados de desempenho, identificando padrões de rechamada e permitindo melhorias contínuas no atendimento.

### 💡 Problema Resolvido

- **Identificação automática** de clientes que retornam ao atendimento
- **Rastreamento** do atendente responsável pela primeira chamada
- **Análise temporal** de rechamadas (mesmo dia, até 3 dias, mais de 3 dias)
- **Métricas de desempenho** por atendente e supervisor
- **Classificação inteligente** dos motivos de rechamada

---

## ✨ Funcionalidades Principais

### 📈 Dashboard Analítico
- Visualização de métricas consolidadas de atendimento
- Gráficos interativos de rechamadas por período
- Filtros dinâmicos por data, operadora e atendente
- Performance comparativa entre equipes

### 🔍 Análise de Rechamadas
- **Detecção automática** de rechamadas (mesmo cliente em período específico)
- **Atribuição inteligente** ao atendente da chamada original
- **Classificação temporal**: Mesmo Dia / Até 3 Dias / Mais de 3 Dias
- **Rastreamento completo** com protocolos e histórico

### 📊 Relatórios de Desempenho
- Tabela de desempenho por atendente
- Taxa de rechamadas individual e por equipe
- Tempo médio de atendimento (TMA)
- Distribuição por motivo de contato

### 🔄 Processamento de Dados
- **Extração automatizada** via API REST
- **Enriquecimento** com DDDs e informações geográficas
- **Deduplicação inteligente** de registros
- **Cache otimizado** para performance

### 📱 Interface Responsiva
- Design adaptável para desktop e mobile
- Exportação de relatórios para Excel/CSV
- Filtros avançados e busca em tempo real

---

## 🛠️ Tecnologias Utilizadas

### Backend
- **Python 3.12+**: Linguagem principal
- **Flask 3.0+**: Framework web
- **Gunicorn**: Servidor WSGI para produção
- **Pandas**: Processamento e análise de dados
- **NumPy**: Computação numérica

### Armazenamento & Cache
- **Parquet**: Formato otimizado para big data
- **CSV**: Fallback e exportação
- **Pickle**: Cache em memória

### Frontend
- **HTML5/CSS3**: Interface web
- **JavaScript**: Interatividade
- **Jinja2**: Template engine

### Integrações
- **API REST**: Extração de dados externos
- **Sistema de autenticação**: Token-based
- **Processamento assíncrono**: Para grandes volumes

---

## 📂 Estrutura do Projeto

```
analise-rechamadas/
├── app.py                    # Aplicação Flask principal
├── rechamada.py              # Script de extração de dados
├── utils.py                  # Funções utilitárias
├── config.py                 # Configurações
├── constants.py              # Constantes e mapeamentos
├── gerar_dados_demo.py       # Gerador de dados mockados
│
├── templates/                # Templates HTML
│   └── dashboard.html
│
├── static/                   # Arquivos estáticos
│   ├── css/
│   ├── js/
│   └── images/
│
├── logs/                     # Logs da aplicação
│
├── dados_consolidado.parquet # Base de dados principal
│
├── requirements.txt          # Dependências Python
├── README.md                 # Este arquivo
└── COLLABORATION.md          # Guia de colaboração
```

---

## 🚀 Funcionalidades Técnicas

### Processamento de Dados
```python
# Exemplo de identificação de rechamadas
- Busca por mesma origem em janela temporal
- Atribuição ao atendente original
- Classificação por tipo e período
- Enriquecimento com contexto geográfico
```

### Cache Inteligente
```python
# Sistema de cache otimizado
- Cache em memória com TTL de 2 horas
- Reload automático sob demanda
- Compartilhamento entre workers Gunicorn
- Flag de força de reload
```

### API de Extração
```python
# Integração com múltiplas APIs
- Autenticação token-based
- Paginação automática
- Retry em caso de falha
- Rate limiting
```

---

## 📊 Métricas e KPIs

O sistema calcula automaticamente:

- **Taxa de Rechamada**: Percentual de clientes que retornam
- **TMA (Tempo Médio de Atendimento)**: Por atendente e equipe
- **Distribuição Temporal**: Rechamadas por período
- **Performance Individual**: Ranking de atendentes
- **Análise de Motivos**: Principais causas de rechamada
- **Tendências**: Evolução ao longo do tempo

---

## 🎨 Interface

### Dashboard Principal
- Cards com métricas consolidadas
- Gráficos de linha para tendências
- Tabelas interativas com ordenação
- Filtros dinâmicos por período

### Tabela de Desempenho
- Listagem de todos os atendentes
- Métricas individuais calculadas
- Ordenação por qualquer coluna
- Exportação para Excel

### Detalhes de Rechamadas
- Histórico completo por atendimento
- Rastreamento de protocolo
- Visualização de causa raiz
- Timeline de interações

---

## 🔒 Segurança

- Autenticação de API com tokens
- Sanitização de inputs
- Logs auditáveis
- Backup automático de dados
- Tratamento de erros robusto

---

## 🎯 Casos de Uso

1. **Gestão de Call Center**
   - Monitoramento de qualidade de atendimento
   - Identificação de treinamentos necessários
   - Otimização de processos

2. **Análise de Performance**
   - Avaliação individual de atendentes
   - Benchmarking entre equipes
   - Estabelecimento de metas

3. **Melhoria Contínua**
   - Identificação de padrões problemáticos
   - Análise de causa raiz
   - Tomada de decisão baseada em dados

---

## 📈 Escalabilidade

- Suporte para **milhões de registros**
- Processamento otimizado com **Pandas vectorization**
- **Cache multinível** para performance
- **Arquitetura stateless** para escalonamento horizontal
- Formato **Parquet** para compressão e velocidade

---

## 🤝 Colaboração

Veja [COLLABORATION.md](COLLABORATION.md) para detalhes sobre como colaborar com o projeto.

---

## 📝 Notas sobre esta Versão

Esta é uma **versão portfolio** do projeto original:
- Dados são completamente mockados/fictícios
- Credenciais e informações sensíveis foram removidas
- Nomes de empresas/operadoras são genéricos
- Configurada para demonstração, não para produção

Para uso em produção real, configure:
- Credenciais de API válidas em `config.py`
- Fontes de dados reais
- Mapeamentos específicos da sua operação
- Ajustes de segurança conforme necessário

---

## 👨‍💻 Autor

Desenvolvido como demonstração de capacidades técnicas em:
- Desenvolvimento web com Python/Flask
- Processamento de big data com Pandas
- Análise de métricas e KPIs
- Integração de APIs
- Design de sistemas escaláveis

---

## 📧 Contato

Para dúvidas sobre a implementação ou interesse em projetos similares, entre em contato via GitHub.

---

<div align="center">

**⭐ Se este projeto foi útil, considere dar uma estrela!**

Desenvolvido com 💙 usando Python

</div>
