import pandas as pd


class DataQualityCheck:
    # guarda o dataframe, o nome da tabela (so pra log) e a lista de resultados
    def __init__(self, df, nome_tabela):
        self.df = df
        self.nome_tabela = nome_tabela
        self.resultados = []

    def check_duplicidade(self, colunas_chave):
        # conta quantas linhas se repetem na combinacao de colunas passada
        dups = int(self.df.duplicated(subset=colunas_chave).sum())
        status = "PASS" if dups == 0 else "FAIL"
        self.resultados.append({
            "dimensao": "Uniqueness",
            "check": "duplicidade",
            "coluna": ",".join(colunas_chave),
            "status": status,
            "detalhe": f"{dups} linhas duplicadas na chave {colunas_chave}",
        })
        return self  # retorna self pra poder encadear (.check_x().check_y())

    def check_nulos(self, coluna, condicao=None):
        # condicao restringe o check a um subconjunto de linhas
        # (ex: nulo so e aceitavel quando ano=2023)
        subset = self.df[condicao] if condicao is not None else self.df
        nulos = int(subset[coluna].isna().sum())
        status = "PASS" if nulos == 0 else "FAIL"
        self.resultados.append({
            "dimensao": "Completeness",
            "check": "nulos",
            "coluna": coluna,
            "status": status,
            "detalhe": f"{nulos} nulos em {len(subset)} linhas avaliadas",
        })
        return self

    def check_fk(self, coluna, valores_validos):
        # confere se todo valor da coluna existe numa lista de referencia
        # (ex: todo id_municipio existe no diretorio geografico)
        invalidos = int((~self.df[coluna].isin(valores_validos)).sum())
        status = "PASS" if invalidos == 0 else "FAIL"
        self.resultados.append({
            "dimensao": "Validity",
            "check": "integridade_referencial",
            "coluna": coluna,
            "status": status,
            "detalhe": f"{invalidos} valores de {coluna} sem correspondencia na tabela de referencia",
        })
        return self

    def check_range(self, coluna, minimo, maximo):
        # confere se os valores estao dentro de uma faixa plausivel
        # (ex: percentual entre 0 e 100)
        mask_fora = self.df[coluna].notna() & ((self.df[coluna] < minimo) | (self.df[coluna] > maximo))
        qtd_fora = int(mask_fora.sum())
        status = "PASS" if qtd_fora == 0 else "FAIL"
        self.resultados.append({
            "dimensao": "Consistency",
            "check": "range",
            "coluna": coluna,
            "status": status,
            "detalhe": f"{qtd_fora} valores fora do intervalo [{minimo},{maximo}]",
        })
        return self

    def score(self):
        # % de checks que passaram, sobre o total de checks rodados
        total = len(self.resultados)
        if total == 0:
            return 100.0
        passou = sum(1 for r in self.resultados if r["status"] == "PASS")
        return round(passou / total * 100, 1)

    def tem_falha(self):
        # True se pelo menos 1 check falhou
        return any(r["status"] == "FAIL" for r in self.resultados)

    def relatorio(self):
        # devolve todos os resultados como uma tabela, pra inspecionar
        return pd.DataFrame(self.resultados)

    def logar(self, logger):
        # imprime cada resultado no log (INFO se passou, WARNING se falhou)
        for r in self.resultados:
            msg = f"[DQ:{self.nome_tabela}] {r['status']} | {r['dimensao']} | {r['check']} | {r['coluna']} | {r['detalhe']}"
            (logger.info if r["status"] == "PASS" else logger.warning)(msg)
        logger.info(f"[DQ:{self.nome_tabela}] Score: {self.score()}%")

    def falhar_se_critico(self):
        # so usar na camada Gold - se algum check falhou, para o pipeline
        # (Bronze/Silver so reportam, nao chamam esse metodo)
        if self.tem_falha():
            falhas = self.relatorio()
            falhas = falhas[falhas["status"] == "FAIL"]
            raise Exception(
                f"[DQ:{self.nome_tabela}] {len(falhas)} check(s) falharam - "
                f"pipeline interrompido (estrategia fail-fast na Gold).\n{falhas.to_string()}"
            )