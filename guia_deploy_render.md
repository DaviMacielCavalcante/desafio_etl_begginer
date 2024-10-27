# Um pequeno guia sobre como fazer o deploy deste projeto no Render

Após a criação da conta, crie:

1. Uma instância do postgres 
2. Coloque as variáveis de ambiente que estão lá em um arquivo local chamado ```.env``` na raiz do projeto.
3. Crie o arquivo ```build.sh```, e adicione a seguinte linha: ```uvicorn main:app --host 0.0.0.0 --port $PORT ``` 
4. Crie um arquivo chamado ```render.yaml``` com as seguintes configurações:
```
services:
  - type: web
    name: desafio-app
    env: python
    buildCommand: |
      pip install -r requirements.txt
    startCommand: |
      uvicorn main:app --host 0.0.0.0 --port 10000
    plan: free
```
5. Faça um conexão a partir da sua máquina com o postgres do render e forneça as seguintes permissões para seu usuário:

```
GRANT CREATE ON SCHEMA public TO seu_usuario;
GRANT USAGE ON SCHEMA public TO seu_usuario;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE telecom, ti, serv_audiovisuais, ed_e_ed_integradas_a_impressao, agencia_noticias TO seu_usuario;
```

6. Rode o script ```criacao_tabelas.py```:

```python criacao_tabelas.py```

7. Rode o seguinte o comando para CADA TABELA do banco:

```
ALTER TABLE nome_da_tabela ADD COLUMN id SERIAL PRIMARY KEY;
```

Por alguma razão, mesmo estando no ```create_tables.sql```, a coluna ```id``` não é criada então esta etapa deve ser executada.

Seguindo esses passos, você pode fazer o deploy do projeto no render, e a API deve funcionar normalmente.


