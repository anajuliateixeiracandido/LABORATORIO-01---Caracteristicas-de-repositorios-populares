import requests
import csv
import time
import random
import os
from datetime import datetime

# Aqui estamos pegando o token do GitHub diretamente das variáveis de ambiente para evitar expor informações sensíveis no código.
TOKEN = os.getenv("GITHUB_TOKEN")
# URL da API GraphQL do GitHub, que será usada para fazer as requisições.
URL = "https://api.github.com/graphql"

# Cabeçalhos necessários para autenticar e especificar o tipo de conteúdo das requisições.
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Query para buscar repositórios populares no GitHub com mais de 1 estrela, ordenados por número de estrelas.
QUERY_REPOS = """
query($first: Int!, $after: String) {
search(
  query: "stars:>1 sort:stars-desc is:public archived:false fork:false",
  type: REPOSITORY,
  first: $first,
  after: $after
) {
  pageInfo { hasNextPage endCursor }
  nodes {
    ... on Repository {
      name
      owner { login }
      createdAt
      pushedAt
      primaryLanguage { name }
      url
    }
  }
}
}
"""

# Query para buscar detalhes adicionais de cada repositório, como pull requests, releases e issues.
QUERY_DETAILS = """
query($owner: String!, $name: String!) {
repository(owner: $owner, name: $name) {
  pullRequests(states: MERGED) { totalCount }
  releases { totalCount }
  issues { totalCount }
  closedIssues: issues(states: CLOSED) { totalCount }
}
}
"""

if __name__ == "__main__":
    # Lista para armazenar os repositórios coletados.
    repos = []
    cursor = None
    total = 0
    # Loop para coletar até 1000 repositórios, em lotes de no máximo 10 por vez.
    while total < 1000:
        batch = min(10, 1000 - total)  # Define o tamanho do lote atual.
        query_vars = {"first": batch, "after": cursor}
        tries = 0
        # Tentativas para lidar com possíveis falhas na requisição.
        while tries < 5:
            try:
                resp = requests.post(URL, headers=HEADERS, json={
                                     "query": QUERY_REPOS, "variables": query_vars})
                if resp.status_code == 200:
                    data = resp.json()
                    # Verifica se houve erros na resposta da API.
                    if "errors" in data:
                        # Espera um pouco antes de tentar novamente.
                        time.sleep(2 + random.random())
                        tries += 1
                        continue
                    break
                else:
                    time.sleep(2 + random.random())
                    tries += 1
            except:
                time.sleep(2 + random.random())
                tries += 1
        # Se todas as tentativas falharem, interrompe o processo.
        if tries == 5 or not resp or "data" not in data or "search" not in data["data"]:
            print("Error fetching repositories")
            break
        result = data["data"]["search"]
        # Adiciona os repositórios coletados ao total.
        repos += result["nodes"]
        total = len(repos)
        if not result["pageInfo"]["hasNextPage"]:
            break
        # Atualiza o cursor para a próxima página.
        cursor = result["pageInfo"]["endCursor"]
        # Pausa para evitar atingir limites da API.
        time.sleep(1 + random.random())

    all_data = []
    for repo in repos:
        owner = repo["owner"]["login"]
        name = repo["name"]
        tries = 0
        # Coleta detalhes adicionais de cada repositório.
        while tries < 5:
            try:
                resp2 = requests.post(URL, headers=HEADERS, json={
                                      "query": QUERY_DETAILS, "variables": {"owner": owner, "name": name}})
                if resp2.status_code == 200:
                    details = resp2.json()
                    # Verifica se houve erros na resposta da API.
                    if "errors" in details:
                        time.sleep(2 + random.random())
                        tries += 1
                        continue
                    break
                else:
                    time.sleep(2 + random.random())
                    tries += 1
            except:
                time.sleep(2 + random.random())
                tries += 1
        # Caso todas as tentativas falhem, define valores padrão para os detalhes.
        if tries == 5 or not resp2 or "data" not in details or "repository" not in details["data"]:
            merged_pull_requests = 0
            releases = 0
            open_issues = 0
            closed_issues = 0
        else:
            repo_details = details["data"]["repository"]
            merged_pull_requests = repo_details["pullRequests"]["totalCount"]
            releases = repo_details["releases"]["totalCount"]
            open_issues = repo_details["issues"]["totalCount"]
            closed_issues = repo_details["closedIssues"]["totalCount"]

        # Verifica se o repositório tem uma linguagem principal definida.
        if repo["primaryLanguage"] is not None:
            lang = repo["primaryLanguage"]["name"]
        else:
            lang = "N/A"

        # Calcula a porcentagem de issues fechadas em relação ao total de issues abertas.
        if open_issues > 0:
            closed_issues_percent = round(
                (closed_issues / open_issues) * 100, 2)
        else:
            closed_issues_percent = 0.0

        # Adiciona os dados coletados em um formato estruturado.
        all_data.append({
            "name": f"{owner}/{name}",
            "url": repo["url"],
            "created_at": repo["createdAt"],
            "updated_at": repo["pushedAt"],
            "language": lang,
            "merged_pull_requests": merged_pull_requests,
            "releases": releases,
            "open_issues": open_issues,
            "closed_issues": closed_issues,
            "closed_issues_percent": closed_issues_percent
        })
        # Pausa para evitar atingir limites da API.
        time.sleep(0.5 + random.random())

    # Define os campos do arquivo CSV e escreve os dados coletados nele.
    fields = [
        "name", "url", "created_at", "updated_at", "language",
        "merged_pull_requests", "releases", "open_issues", "closed_issues", "closed_issues_percent"
    ]
    with open("github_repositories.csv", "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fields)
        csv_writer.writeheader()  # Escreve o cabeçalho no arquivo CSV.
        for repository_row in all_data:
            # Escreve cada linha de dados no arquivo.
            csv_writer.writerow(repository_row)
