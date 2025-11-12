# 🤝 Guia de Colaboração

Obrigado pelo interesse em explorar este projeto! Este guia explica como você pode entender, testar e potencialmente contribuir com melhorias.

---

## 📋 Sobre Esta Versão

Este é um projeto **portfolio/demonstração**:
- ✅ Código real de produção (sanitizado)
- ✅ Dados mockados para demonstração
- ✅ Funcionalidades completas e testáveis
- ❌ Não conectado a APIs reais
- ❌ Dados fictícios (não representam operações reais)

---

## 🚀 Como Explorar o Projeto

### 1. Pré-requisitos

```bash
- Python 3.12 ou superior
- pip (gerenciador de pacotes Python)
- Git
```

### 2. Clonar e Configurar

```bash
# Clone o repositório
git clone https://github.com/SEU_USUARIO/analise-rechamadas.git
cd analise-rechamadas

# Crie um ambiente virtual
python3 -m venv venv

# Ative o ambiente virtual
# Linux/Mac:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# Instale as dependências
pip install -r requirements.txt
```

### 3. Gerar Dados de Demonstração

```bash
# Execute o gerador de dados mockados
python gerar_dados_demo.py
```

Isso criará:
- `dados_consolidado.parquet` com 5.000 registros fictícios
- Dados de 8 operadoras mockadas
- 50 atendentes fictícios
- Rechamadas simuladas (15% dos contatos)

### 4. Executar a Aplicação

```bash
# Modo desenvolvimento
python app.py

# Ou com Flask direto
flask run --host=0.0.0.0 --port=5000
```

Acesse: `http://localhost:5000`

---

## 🔍 Explorando as Funcionalidades

### Dashboard Principal (`/dashboard` ou `/`)
- Visualize métricas consolidadas
- Teste filtros de data
- Explore tabelas interativas
- Veja gráficos de tendências

### Análise de Rechamadas
- Identifique padrões de rechamada
- Veja atribuição de responsabilidade
- Analise tipos de rechamada
- Explore classificação temporal

### Exportação de Dados
- Teste a funcionalidade de exportar relatórios
- Visualize dados em Excel/CSV
- Explore diferentes formatos de saída

---

## 📚 Entendendo o Código

### Arquitetura

```
API Externa → rechamada.py → Processamento (utils.py) → Cache → Flask (app.py) → Templates HTML
```

### Arquivos Principais

**app.py**
- Aplicação Flask
- Rotas e endpoints
- Sistema de cache
- Geração de relatórios

**rechamada.py**
- Extração de dados via API
- Processamento por lotes
- Controle de paginação

**utils.py**
- Funções de processamento
- Análise de rechamadas
- Enriquecimento de dados
- Mapeamentos e transformações

**config.py**
- Configurações gerais
- Credenciais (mockadas nesta versão)
- Parâmetros de filtro

**constants.py**
- Mapeamentos de operadoras
- Classificação de motivos
- Listas de validação

---

## 🛠️ Áreas para Exploração/Contribuição

### Performance
- Otimizações de queries Pandas
- Melhorias no sistema de cache
- Paralelização de processamento

### Funcionalidades
- Novos tipos de relatórios
- Análises estatísticas adicionais
- Gráficos e visualizações

### Interface
- Melhorias de UX/UI
- Responsividade mobile
- Acessibilidade

### Testes
- Testes unitários
- Testes de integração
- Testes de performance

---

## 💡 Sugestões de Melhorias

Se você quiser experimentar melhorias:

### Fácil
- Adicionar novos gráficos ao dashboard
- Melhorar o design visual
- Adicionar mais filtros

### Médio
- Implementar cache Redis
- Adicionar autenticação de usuário
- Criar API REST para consultas

### Avançado
- Adicionar machine learning para predição de rechamadas
- Implementar processamento assíncrono com Celery
- Criar versão em React/Vue para frontend

---

## 🐛 Reportando Problemas

Se encontrar algum bug ou comportamento inesperado:

1. Verifique se os dados mockados foram gerados corretamente
2. Confira se todas as dependências foram instaladas
3. Revise os logs em `logs/aplicacao_app.log`
4. Abra uma issue descrevendo:
   - O que você esperava
   - O que aconteceu
   - Passos para reproduzir
   - Mensagens de erro (se houver)

---

## 📖 Documentação Técnica

### Stack Técnico

**Backend**
- Flask: Framework web
- Pandas: Processamento de dados
- Gunicorn: Servidor WSGI (produção)

**Armazenamento**
- Parquet: Dados estruturados
- CSV: Fallback e exportação
- Pickle: Cache temporário

**Frontend**
- Jinja2: Templates
- HTML/CSS/JS: Interface

### Padrões de Código

- PEP 8 para código Python
- Docstrings em funções importantes
- Comentários para lógica complexa
- Logging estruturado

### Estrutura de Dados

Os dados seguem este schema:

```python
{
    'data_hora_contato': datetime,
    'protocolo': str,
    'origem': str (telefone),
    'mvno': str (operadora),
    'l5_agente': str (ID do atendente),
    'nome_agente': str,
    'supervisor': str,
    'tempo_atendimento': int (segundos),
    'tempo_espera': int (segundos),
    'tempo_ligacao_total': int (segundos),
    'motivo_categoria': str,
    'is_rechamada': bool,
    'causou_rechamada': bool,
    'tipo_rechamada': str,
    ...
}
```

---

## 🔧 Configuração para Testes

### Variando os Dados

Edite `gerar_dados_demo.py` para:
- Mudar quantidade de registros (`n_registros`)
- Ajustar taxa de rechamadas
- Modificar período de dados
- Adicionar/remover operadoras

```python
# Exemplo: aumentar para 10.000 registros
n_registros = 10000

# Exemplo: aumentar taxa de rechamadas para 20%
if random.random() < 0.20:  # 20%
    registro['is_rechamada'] = True
```

### Testando Performance

```bash
# Gere dados maiores
python gerar_dados_demo.py  # modifique n_registros antes

# Teste tempo de carregamento
time python app.py
```

---

## 📬 Contato

Este projeto é mantido como portfolio. Para questões técnicas ou profissionais:

- Abra uma issue no GitHub
- Conecte-se via LinkedIn
- Envie um email

---

## 🙏 Agradecimentos

Obrigado por explorar este projeto! Qualquer feedback é bem-vindo.

---

## 📄 Licença

Este projeto é disponibilizado para fins de demonstração e portfolio. Sinta-se livre para estudar, aprender e se inspirar no código.

---

<div align="center">

**Desenvolvido com 💙 e Python**

⭐ **Star** este projeto se você achou interessante!

</div>
