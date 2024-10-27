# Documentação da API (main.py)

Esta API foi desenvolvida para operações CRUD em tabelas específicas de um banco de dados PostgreSQL. Abaixo está a descrição detalhada de cada endpoint, os modelos de dados usados e como configurar a aplicação para deploy.

---

## Endpoints

### 1. Criar nova entrada de dados

- **URL**: `/data/{table}/`
- **Método**: `POST`
- **Descrição**: Insere uma nova entrada na tabela especificada.
- **Parâmetros**:
  - `table` (path): Nome da tabela (ex.: `telecom`, `ti`, `serv_audiovisuais`).
- **Corpo da Requisição**:
  ```json
  {
      "ano": 2022,
      "receita_liquida": 5000.00,
      "custo_mercadorias": 1500.00,
      ...
  }
    ```

Resposta: `201 Created`, retorna a entrada criada.

2. Listar todas as entradas de uma tabela: 

- **URL**: `/data/{table}/`
- **Método**: `GET`
- **Descrição**: Retorna uma lista de todas as entradas de uma tabela.
- **Parâmetros**:
    - `table` (path): Nome da tabela.

- **Resposta**: `200 OK`, retorna uma lista de objetos `DataEntry`.

3. Obter entrada por ano

- **URL**: `/data/{table}/{year}`
- **Método**: `GET`
- **Descrição**: Retorna uma entrada específica de um ano em uma tabela.
- **Parâmetros**:
    - `table` (path): Nome da tabela.
    - `year` (path): Ano específico.

- **Resposta**: `200 OK`, retorna o objeto `DataEntry`.

4. Atualizar entrada por ano

- **URL**: `/data/{table}/{year}`
- **Método**: `PUT`
- **Descrição**: Atualiza uma entrada existente com os dados fornecidos.
- **Parâmetros**:
    - `table` (path): Nome da tabela.
    - `year` (path): Ano específico da entrada a ser atualizada.
- **Corpo da Requisição**:
```json
{
    "ano": 2022,
    "receita_liquida": 5500.00,
    ...
}
```

**Resposta**: `200 OK`, retorna a entrada atualizada.

5. Excluir entrada por ano

- **URL**: `/data/{table}/{year}`
- **Método**: `DELETE`
- **Descrição**: Remove uma entrada específica de um ano em uma tabela.
- **Parâmetros**:
    - `table` (path): Nome da tabela.
    - `year` (path): Ano específico.
- **Resposta**: `200 OK`, retorna uma mensagem de confirmação.
**Modelos de Dados**: `DataEntry`
Modelo usado para representar e validar os dados de entrada e saída da API.

```json
    {
        "ano": int,
        "receita_liquida": float,
        "custo_mercadorias": float,
        "subvencoes_receitas_op": float,
        "valor_bruto_producao": float,
        "consumo_intermediario_total": float,
        "consumo_combustiveis": float,
        "numero_empresas": float
    }
```

### Estrutura das Tabelas
Todas as tabelas possuem uma estrutura base similar, com campos de dados financeiros, consumo e número de empresas.


#### Exemplo de Uso

1. Criar uma nova entrada:

```bash
curl -X POST "http://localhost:8000/data/telecom/" -d '{
    "ano": 2023,
    "receita_liquida": 4500.0,
    ...
}'
```
2. Listar todas as entradas de uma tabela:

```bash
curl -X GET "http://localhost:8000/data/telecom/"
```

3. Obter dados por ano:

```bash
curl -X GET "http://localhost:8000/data/telecom/2022"
```
4. Atualizar dados:

```bash
curl -X PUT "http://localhost:8000/data/telecom/2022" -d '{
    "ano": 2022,
    "receita_liquida": 4800.0,
    ...
}'
```
5. Excluir dados:

```bash
curl -X DELETE "http://localhost:8000/data/telecom/2022"
```
*Through victory, my chains are broken. The Force shall free me.*