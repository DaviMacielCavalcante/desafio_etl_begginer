# Desafio ETL Beginner

![Licença usada](https://img.shields.io/github/license/DaviMacielCavalcante/desafio_etl_begginer)
![Python](https://img.shields.io/badge/Python-3.12.4-blue)
![Status](https://img.shields.io/badge/Status-Finalizado-brightgreen)
![Banco de Dados](https://img.shields.io/badge/Banco%20de%20Dados-PostgreSQL-blue)


## Descrição:
Este projeto foi desenvolvido como parte de um desafio de ETL para iniciantes. O objetivo é extrair, transformar e carregar dados de um arquivo csv e carregá-los em um banco de dados local.  Neste projeto foi utilizado o PostgreSQL.

## Funcionalidades:
- Extração de dados de um arquivo csv
- Transformação de dados com limpeza e padronização
- Carregamento de dados em um banco de dados relacional (postgresSQL)
- Funcionalidades de um crud via API com FastAPI

## Instalação:

1. Clone este repositório:
   ```bash
   git clone https://github.com/DaviMacielCavalcante/desafio_etl_begginer.git
   cd desafio_etl_begginer
2. Instale as dependências do arquivo requirements.txt
   ```bash
   pip install -r requirements.txt
3. Instale o postgres na sua máquina
4. Faça um arquivo chamado .env com as seguintes variáveis
```
DATABASE_PORT = porta que o seu banco usar
DABASE_NAME = nome que você deu ai banco
DATABASE_USERNAME = usuário que tem acesso ao banco
DATABASE_PASSWORD = senha do usuário
DATABASE_URL = endereço do seu banco 
```
5. Execute o scrip que é responsável por fazer a criação das tabelas no postgres
   ```bash
   python criacao_tabelas.py
6. Execute o script responsável pela pipeline de ETL
   ```bash
   python main.py
7. Veja o lado legal da força em ação:

<div align="center"> <img src="https://www.icegif.com/wp-content/uploads/2022/09/icegif-1012.gif" alt="darth_vader_local_nevando" width="500"/></div>

## Como contribuir:
Contribuições são bem-vindas! Por favor, siga estas diretrizes:

- Faça um fork do projeto.
- Crie uma branch para a funcionalidade que deseja implementar (git checkout -b minha-nova-feature).
- Faça os commits com boas descrições (git commit -m 'Adiciona nova feature').
- Faça um push para a branch criada (git push origin minha-nova-feature).
- Abra um pull request para revisão.

## Licença
Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE.md para mais detalhes.

## Contato
Se você tiver alguma dúvida ou problema, entre em contato:

e-mail: davicc@outlook.com.br

## Lordes Sith responsáveis pelo projeto
- Darth Davi ⚔️😡

## Mentora que propôs o desafio:
[Profa. Artemisia Weyl](https://www.linkedin.com/in/arteweyl/)

Github da mentora: https://github.com/arteweyl

*Through victory, my chains are broken.
<br>
The Force shall free me.*
