## Построение инвертированного индекса и реализация булева поиска в рамках курса по информационному поиску.


**1. Сбор корпуса.**

Корпус, содержащий описания фильмов с Википедии, взят с [kaggle](https://www.kaggle.com/jrobischon/wikipedia-movie-plots#wiki_movie_plots_deduped.csv). 
В качестве документа рассматривается строка, поиск производится только по секции Plot (сюжета). 

**2. Токенизация.**

На этапе токенизации текста из всех текстов были убраны **html-тэги** и **знаки пунктуации**, после чего текст был разбит на токены по пробельным символам.

**3. Лексическая обработка текста.**

Производится **лемматизация** токенов при использовании **nltk.WordNetLemmatizer()**.

**4. Построение инвертированного индекса.**

Инвертированный индекс строится на основе алгоритма [SPIMI](https://nlp.stanford.edu/IR-book/html/htmledition/single-pass-in-memory-indexing-1.html). 
Для хранения индекса в памяти используется defaultdict(), выгрузка производится при помощи **ndjson** в .json файлы в директориях data/.
Для конечного ранжирования при выводе результатов поиска в индексе также хранятся частоты терма в документе, то есть запись имеет структуру **doc_id:tf**

Построенные блоки индекса мерджатся последовательно при помощи [merge-sort алгоритма](https://nlp.stanford.edu/IR-book/html/htmledition/blocked-sort-based-indexing-1.html),запись производится в директорию data/files_index в качестве нескольких .json файлов, названием которых является терм, для улучшения поиска .

Реализованы операции над *posting lists*: **AND**, **OR**, **NOT**. 

В качестве ответа выдаётся общее количество найденных документов по запросу и далее TOP_CUT = 5 (можно менять в constants.py) документы, отранжированные по tf, хранящейся в индексе.
Проверить правильность ответа можно перейдя по ссылке на Википедию (она содержится в ответе), и Ctrl+F по секции Plot.

**Примеры запросов, валидные формату** (слово - операция - слово):

**one-word**:

fight

help

**and**:

cska and coach

moon and shining

**or**:

moon or rain

**not**:

cska not

help not










