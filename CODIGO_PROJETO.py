### pip install pandas
### pip install spacy
### pip install nltk
### pip install praw
### pip install time
### pip install re
### pip install json
### pip install logging
### pip install datetime
### pip install langdetect

### python -m spacy download pt_core_news_sm
### python -c "import nltk; ntlk.download("stopwords")"


import pandas as pd
import spacy
import nltk
import praw
import time
import re
import json
import logging
from datetime import datetime, UTC
from langdetect import detect, LangDetectException


class RedditDados():
    """
    RedditDados é uma classe que possibilita a conexão e a coleta de dados com a API do Reddit.
    """
    
    def __init__(self, ID: str, CLIENTE: str, AGENTE: str):
        """
        __init__ irá iniciar o objeto RedditDados, realizando a autenticação com a API do Reddit.

        Args:
            ID (str): O ID do cliente para a API do Reddit.
            CLIENTE (str): O segredo do cliente para a API do Reddit.
            AGENTE (str): O user agent para a requisição à API.
        """
        
        print("Passo 1: Configurando e conectando à API do Reddit")
        
        self.reddit = praw.Reddit(
            client_id = ID,
            client_secret = CLIENTE,
            user_agent = AGENTE
        )
        self.dados_brutos = []
        self.ids_coletados = set()


    def busca_de_dados(self, string_busca: str, contador_posts: int) -> list:
        """
        busca_de_dados é uma função que executa um processo exaustivo de busca no Reddit sobre um determinado termo.
        Ela utiliza diferentes tipos de ordenações e de filtros de tempo para possibilitar o maior número de resultados únicos a serem retornados pela API.
        
        Args:
            string_busca (str): Seria uma string da busca a ser feita na API.
            contador_posts (int): Seria o número desejado de posts a serem coletados.

        Returns:
            (list): Uma lista de dicionários, onde cada um dos dicionários representa um titulo de post bruto com 'id_post', 'data_utc' e 'titulo'.
        """

        print()
        print(f"Passo 2: Coleta Exaustiva dos Dados (Alvo: {contador_posts} posts)")
        print("...Observação: Isso pode demorar muito :(")
        
        tipos_de_busca = ["relevance", "new", "hot", "top"]
        filtros_de_tempo = ["all", "year", "month", "week", "day"]

        for tipo_ordenacao in tipos_de_busca:
            if len(self.ids_coletados) >= contador_posts:
                break

            if tipo_ordenacao in ["new", "hot", "relevance"]:
                filtro_ordenacao = ["all"]
            else:
                filtro_ordenacao = filtros_de_tempo

            for filtro_tempo in filtro_ordenacao:
                if len(self.ids_coletados) >= contador_posts:
                    break

                print()
                print(f"Buscando: sort = \"{tipo_ordenacao}\", filtro_tempo = \"{filtro_tempo}\"...")
                
                try:
                    resultados = self.reddit.subreddit("all").search(
                        string_busca, sort = tipo_ordenacao, time_filter = filtro_tempo, limit = 1000
                    )
                    contagem_anterior = len(self.ids_coletados)
                    for post in resultados:
                        if post.id not in self.ids_coletados:
                            self.dados_brutos.append({
                                "id_post": post.id,
                                "data_utc": datetime.fromtimestamp(post.created_utc, UTC).strftime("%Y-%m-%d %H:%M:%S"), 
                                "titulo": self._limpeza_de_texto(post.title)
                            })
                            self.ids_coletados.add(post.id)
                    contagem_posterior = len(self.ids_coletados)
                    print(f"Encontrados: {contagem_posterior - contagem_anterior} novos posts nesta busca")
                    print(f"Total da coleta (até agora): {len(self.ids_coletados)}/{contador_posts}")
                except Exception as e:
                    print(f"Ocorreu um ERRO: {e}")
                time.sleep(2)

        print()
        print(f"Coleta de Dados Finalizada: {len(self.dados_brutos)} posts brutos únicos encontrados")

        return self.dados_brutos


    def _limpeza_de_texto(self, texto: str) -> str:
        """
        _limpeza_de_texto é uma função para realizar a limpeza básica de uma string.
        """
        
        if not texto:
            return ""
        texto = str(texto).replace("\n", " ").replace("\r", " ")
        texto = re.sub(r"\s+", " ", texto)
        return texto.strip()


class ProcessamentoDosDados():
    """
    ProcessamentoDosDados é uma classe que fica responsável por receber uma lista de dados brutos e aplicar algumas regras de "pré-limpeza" e processamento, utilizando a biblioteca Pandas
    A classe transforma os dados da coleta em uma base mais limpa e estruturada, pronta para ser utilizada em um Processamento Linguistico.
    """
    
    def __init__(self, dados_brutos: list):
        """
        __init__ começa o processamento dessa lista de dados brutos.
        
        Args:
            dados_brutos (list): Seria a lista de posts que foram coletados.
        """

        print()
        print("Passo 3: Iniciando este Pré-Processamento de Dados")

        if not dados_brutos:
            self.df_bruto = pd.DataFrame()
        else:
            self.df_bruto = pd.DataFrame(dados_brutos)
        
        print(f"DataFrame 'bruto' criado")


    def pre_limpeza(self, lista_keywords: list) -> pd.DataFrame:
        """
        pre_limpeza é uma função que filtra por Keywords e remove possíveis duplicatas.
        Esta função chama algumas funções internas para filtrar o DataFrame por palavras-chave e remove posts com títulos duplicados.

        Args:
            lista_keywords (list): É uma lista de keywords para filtrar os títulos.

        Returns:
            (pd.DataFrame): Seria um DataFrame limpo e pré-tratado, pronto para ser analisado.
        """
        
        if self.df_bruto.empty:
            print("DataFrame de entrada está vazio")

            return pd.DataFrame()

        df_filtrado = self._filtrar_keyword(lista_keywords)
        df_final = self._remover_duplicatas(df_filtrado)
        
        return df_final


    def _filtrar_keyword(self, lista_keywords: list) -> pd.DataFrame:
        """
        _filtrar_keyword é uma função (método privado, se formos mais precisos) que filtra o DataFrame, de modo a filtrar possíveis keywords.

        Args:
            lista_keywords (list): A lista de keywords a serem buscadas nos títulos.

        Returns:
            (pd.DataFrame): Seria um novo DataFrame que contenha apenas os títulos que passaram pelo critério de filtragem.
        """
        
        self.df_bruto["titulo_lower"] = self.df_bruto["titulo"].str.lower()

        def titulo_keyword(titulo: str, lista_keywords: list) -> bool:
            """
            Função que verifica se alguma das keywords está presente no titulo.
            Retorna True se encontrar, e False caso contrário.
            """

            for keyword in lista_keywords:
                if keyword in titulo:
                    return True
                else:
                    return False

        indice = self.df_bruto["titulo_lower"].apply(
            lambda titulo: titulo_keyword(titulo, lista_keywords)
        )

        df_filtrado = self.df_bruto[indice].copy()
        
        print(f"Após a filtragem por keywords, restaram {len(df_filtrado)} posts")
        
        return df_filtrado


    def _remover_duplicatas(self, df_filtrado: pd.DataFrame) -> pd.DataFrame:
        """
        _remover_duplicatas é uma função que remove os posts com títulos duplicados, mantendo a primeira ocorrência.

        Args:
            df_filtrado (pd.DataFrame): Seria o DataFrame resultante da filtragem das keywords.

        Returns:
            (pd.DataFrame): Seria um DataFrame sem títulos duplicados e pronto para a próxima etapa.
              'titulo_lower' removida, pronto para a próxima etapa.
        """
        
        if df_filtrado.empty:
            return pd.DataFrame()

        df_filtrado["data_utc"] = pd.to_datetime(df_filtrado["data_utc"])
        df_filtrado = df_filtrado.sort_values("data_utc", ascending = True)
        df_final = df_filtrado.drop_duplicates(subset = ["titulo"], keep = "first")

        print(f"Após a remoção de títulos duplicados, restaram {len(df_final)} posts")

        df_final = df_final.drop(columns = ["titulo_lower"])
        
        return df_final


class ProcessamentoLinguistico():
    """
    ProcessamentoLinguistico é uma classe que é responsável pelo processamento linguístico usando as bibliotecas spaCy e NLTK, além de outras que tratem de PLN.
    A classe utiliza o spaCy para análises profundas, e bibliotecas como NLTK e langdetect para tarefas auxiliares, como remoção de stopwords e detecção de idioma.
    """
    
    def __init__(self, modelo: str):
        """
        __init__ é o que inicializa a análise, carregando o modelo do spaCy e a lista de stopwords.
        Ela carrega o modelo de linguagem e a lista de stopwords do NLTK, preparando para a análise linguistica.

        Args:
            modelo (str): Seria o modelo spaCy utilizado no processamento linguistico.
        """
        
        print()
        print("Passo 4: Preparando o ambiente para a Análise Linguística")
        
        try:
            self.nlp = spacy.load(modelo)
            
            print(f"Modelo '{modelo}' do spaCy carregado com sucesso.")
            
        except OSError:
            print(f"ERRO: Modelo '{modelo}' não encontrado. Execute: python -m spacy download {modelo}")

            self.nlp = None
            
        try:
            self.stopwords = nltk.corpus.stopwords.words('portuguese')
            
        except LookupError:
            print("Baixando a lista de stopwords do NLTK")
            
            nltk.download('stopwords')
            self.stopwords = nltk.corpus.stopwords.words('portuguese')

    def analisar_corpus(self, df_limpo: pd.DataFrame) -> pd.DataFrame:
        """
        analisar_corpus é a função principal que irá fazer todo o processamento linguistico:
        primeiro, irá fazer a filtragem dos titulos pelo idioma de interesse;
        segundo, fará o pré-processamento dos títulos a partir das stopwords;
        e por fim, irá devidamente criar o corpus, ao fazer a análise minunciosa com o spaCy.

        Args:
            df_limpo (pd.DataFrame): Seria o DataFrame de entrada, contendo os dados a serem analisados.

        Returns:
            (pd.DataFrame): Um novo DataFrame, onde cada uma das linhas representam um único token de um título, e sua "classificação" linguistica.
        """
        if self.nlp is None:
            print("O modelo spaCy não foi carregado. A análise não pode continuar.")
            return pd.DataFrame()

        df_filtrado_idioma = self._filtragem_do_idioma(df_limpo)
        if df_filtrado_idioma.empty:
            return pd.DataFrame()

        df_com_titulo_limpo = self._normalizacao_de_titulos(df_filtrado_idioma)
        df_linguistico = self._pipeline_processamento(df_com_titulo_limpo)

        return df_linguistico


    def _analise_do_idioma(self, titulo: str) -> str:
        """
        _analise_do_idioma é um método privado que irá detectar o idioma de um titulo.
        Ele utiliza a biblioteca langdetect para analisar se o idioma do titulo está em português.

        Args:
            titulo (str): É a string de um título a ser analisado

        Returns:
            (str): Seria o código do idioma detectado, ou "desconhecido"/"erro"
        """
        
        try:
            if titulo and len(str(titulo).strip()) > 10:
                return detect(titulo)
            return 'desconhecido'
        except LangDetectException:
            return 'erro'


    def _filtragem_do_idioma(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        _filtragem_do_idioma é uma função que irá aplicar a filtragem do idioma no DataFrame.
        Assim, ela itera sobre o título, detecta o idioma de cada um e retorna o DataFrame contendo apenas os títulos em português.

        Args:
            df (pd.DataFrame): Seria o DataFrame de entrada.

        Returns:
            (pd.DataFrame): Um novo DataFrame contendo os títulos de posts em português.
        """
        
        print()
        print("Passo 5: Filtrando posts para manter apenas os de língua portuguesa...")
        df['idioma'] = df['titulo'].apply(self._analise_do_idioma)
        df_filtrado = df[df['idioma'] == 'pt'].copy()
        
        posts_removidos = len(df) - len(df_filtrado)
        print(f"Filtragem por idioma concluída. {posts_removidos} posts removidos.")
        
        if df_filtrado.empty:
            print("Nenhum post em português foi encontrado após o filtro.")
        
        return df_filtrado


    def _normalizacao_de_titulos(self, df: pd.DataFrame) -> pd.DataFrame:
        """'.
        _normalizacao_de_titulos é uma função que aplica uma filtragem linguística dos dados.
        Ela normaliza os titulos (deixando em minúsculo), e remove as pontuações e as stopwords.

        Args:
            df (pd.DataFrame): Seria o DataFrame que está sendo processado.

        Returns:
            (pd.DataFrame): Seria o mesmo DataFrame de entrada, mas agora com a adição de uma coluna com o título limpo.
        """

        
        def limpeza(titulo):
            titulo = str(titulo).lower()
            titulo = re.sub(r'[^\w\s]', '', titulo)
            palavra_filtrada = []
            lista_de_palavras = titulo.split()
            for p in lista_de_palavras:
                if p not in self.stopwords:
                    palavra_filtrada.append(p)
            return ' '.join(palavra_filtrada)
            
        df['titulo_limpo'] = df['titulo'].apply(limpeza)
        return df


    def _pipeline_processamento(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        _pipeline_processamento é um método privado que aplica a pipeline de processamento linguistico do spaCy.
        Ou seja, é um método que processa o título e faz um processo detalhado com o token, incluindo lema, classe gramatical, dependência sintática e reconhecimento de entidade nomeada.

        Args:
            df (pd.DataFrame): Seria o DataFrame que contém os titulos a serem analisados.

        Returns:
            (pd.DataFrame): Um novo DataFrame, com cada linha representando um único token e suas associações linguisticas.            
        """
        
        print()
        print(f"Passo 6: Iniciando análise com o spaCy em {len(df)} títulos")
        
        lista_de_tokens = []
        for i, linha in df.iterrows():
            doc = self.nlp(linha['titulo'])
            for token in doc:
                lista_de_tokens.append({
                    'id_post': linha['id_post'],
                    'data_utc': linha['data_utc'],
                    'titulo_original': linha['titulo'],
                    'titulo_limpo': linha['titulo_limpo'],
                    'token': token.text,
                    'lemma': token.lemma_,
                    'pos_universal': token.pos_,
                    'pos_detalhada': token.tag_,
                    'dependencia_sintatica': token.dep_,
                    'palavra_mae': token.head.text,
                    'entidade_nomeada': token.ent_type_ if token.ent_type_ else None
                })
        
        print("Processamento Linguistico Completo")
        return pd.DataFrame(lista_de_tokens)


class GerenciadorDeArquivos():
    """
    GerenciadorDeArquivos é uma classe que irá lidar com possíveis operações com arquivos.
    """
    
    @staticmethod
    def salvar_JSON(dados_salvar: list, nome_arquivo: str):
        """
        salvar_JSON é uma função que salva uma lista de dicionários em um arquivo formatado .JSON.

        Args:
            dados_salvar (list): Seria uma lista de dicionários a ser salva em um arquivo.
            nome_arquivo (str): Simplesmente o nome do arquivo .JSON de saída.
        """
        
        if not dados_salvar:
            print(f"Nenhum dado fornecido para salvar em '{nome_arquivo}'.")
            return
        
        with open(nome_arquivo, "w", encoding = "utf-8") as f:
            json.dump(dados_salvar, f, ensure_ascii = False, indent = 4)

    
    @staticmethod
    def salvar_CSV(df: pd.DataFrame, nome_arquivo: str):
        """
        Salva um DataFrame em um arquivo CSV.
        salvar_CSV é uma função que irá salvar um DataFrame em um arquivo .CSV.

        Args:
            df (pd.DataFrame): Seria o DataFrame final, e que contém os dados a serem salvos.
            nome_arquivo (str): Simplesmente o nome do arquivo .CSV de saída.
        """

        print()
        print(f"Passo 7: Salvando o corpus em '{nome_arquivo}'")
        if df.empty:
            print("Não há dado nenhum para ser salvo.")
            return
        
        df.to_csv(nome_arquivo, index = False, encoding = 'utf-8')
        print("Processo Finalizado!")


def main():
    # Definição de Variáveis
    TERMO = "'Petrobras' OR 'PETR4'"
    PALAVRA_CHAVE = ["petrobras", "petr4"]
    Q_POSTS = 1000
    NOME_MODELO = 'pt_core_news_sm'
    NOME_ARQUIVO = f"CORPUS_PETROBRAS.csv"

    # Parte 1: Configurando a API
    coleta = RedditDados("mlUH2P2gnW3U_WhUWm2tkA", "j4aDeGrINcKp_8sc4SXX6NeKWJD7Kg", "Dazzling_Piece_2882")
    dados_brutos = coleta.busca_de_dados(TERMO, Q_POSTS)

    # Parte 2: Pré-Processamento de Dados
    pre_processamento = ProcessamentoDosDados(dados_brutos)
    df_pre_processamento = pre_processamento.pre_limpeza(PALAVRA_CHAVE)

    if df_pre_processamento.empty:
        print()
        print("Nenhum dado restou após a limpeza inicial")
        print("Encerrando")
        return

    # Parte 3: Processamento Linguistico
    analise_linguistica = ProcessamentoLinguistico(NOME_MODELO)
    df_linguistico = analise_linguistica.analisar_corpus(df_pre_processamento)

    # Parte 4: Geração de Arquivo
    GerenciadorDeArquivos.salvar_CSV(df_linguistico, NOME_ARQUIVO)

    
if __name__ == "__main__":
    main()