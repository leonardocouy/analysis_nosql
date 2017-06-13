from pymongo import MongoClient

import feedparser
import string
import html  # Convert HTML accents to text
import unidecode  # Remove accents
import re

client = MongoClient('localhost', 27017)
db = client.aula
news = db.news

# 1. Parseamos o RSS diante da URL do RSS do EM
em_rss = feedparser.parse('http://www.em.com.br/rss/noticia/gerais/rss.xml')

# 2. Vamos iterar por todos os registros e extrair o sumário de todas as notícias. (list comprehesion)
summary_list = [news.summary for news in em_rss.entries]

# 3. Verificamos o número de sumários extraídos
print('Número de sumários extraídos: {total}'.format(total=len(summary_list)))

# 4. Vamos iterar por cada registro, remover as pontuações e inserir no banco.
for description in summary_list:
    cleaned_description = html.unescape(description) # Remove HTML characters
    cleaned_description = unidecode.unidecode(cleaned_description).translate(string.punctuation) # Remove accents
    cleaned_description = re.sub(r"[^a-zA-Z]+", " ", cleaned_description) # Remove SPECIAL Characters
    news.insert_one({'description': cleaned_description})

# Checamos se foram inseridos os registros no banco
print(news.count())
print(news.find_one())



######## PYTHONIC WAY
words = []

# Pegamos todos os registros
description_list = news.find()

# Quebramos todas as frases em palavras.
for word in description_list:
    words.extend(word["description"].split())

print(words)
print('Número de ocorrências: {num}'.format(num=len(words)))


######## MONGO WAY

from bson.code import Code

map = Code("""
            function () {
              this.description.trim().split(/\s+/).forEach((z) => { emit(z, 1) });
            };
           """)

reduce = Code("""
                function(key, values) {
                    return Array.sum(values)
                }
              """)

result = news.map_reduce(map, reduce, "wordcount")


###### EXIBICAO DOS RESULTADOS

# Python Way
word_freq = {}

for word in words:
    # Se a palavra não estiver no dicionário de frequencia, então adicione ela e a frequência.
    if not word in word_freq.keys():
        word_freq.update({word: words.count(word)})

print(word_freq)

# MongoDB Way
for words in db.wordcount.find():
    print(words)