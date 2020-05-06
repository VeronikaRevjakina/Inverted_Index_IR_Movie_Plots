## Построение инвертированного индекса и реализация булева поиска в рамках курса по информационному поиску.


**1. Сбор корпуса.**

Корпус, содержащий описания фильмов с Википедии, взят с [kaggle](https://www.kaggle.com/jrobischon/wikipedia-movie-plots#wiki_movie_plots_deduped.csv). 
В качестве документа рассматривается строка, поиск производится только по секции Plot (сюжета). 
**Важно** файл с данными должен располагаться в корневой директории ( с оновным файлом build_index.py).

**2. Токенизация.**

На этапе токенизации текста из всех текстов были убраны **html-тэги** и **знаки пунктуации**, после чего текст был разбит на токены по пробельным символам.

**3. Лексическая обработка текста.**

Производится **лемматизация** токенов при использовании **nltk.WordNetLemmatizer()**.

**4. Построение инвертированного индекса.**

Инвертированный индекс строится на основе алгоритма [SPIMI](https://nlp.stanford.edu/IR-book/html/htmledition/single-pass-in-memory-indexing-1.html). 
Для хранения индекса в памяти используется defaultdict(), выгрузка производится при помощи **ndjson** в .json файлы в директориях data/.
Для конечного ранжирования при выводе результатов поиска в индексе также хранятся частоты терма в документе, то есть запись имеет структуру **doc_id:tf**

Построенные блоки индекса мерджатся последовательно при помощи [merge-sort алгоритма](https://nlp.stanford.edu/IR-book/html/htmledition/blocked-sort-based-indexing-1.html),запись производится в директорию data/files_index в качестве нескольких .json файлов, названием которых является терм, для улучшения поиска .

[Реализованы операции над *posting lists*](https://nlp.stanford.edu/IR-book/html/htmledition/processing-boolean-queries-1.html#sec:postingsintersection): **AND**, **OR**, **NOT**, **MULTIPLE AND**, **ORNOT**, **ANDNOT**. 

В качестве ответа выдаётся общее количество найденных документов по запросу и далее TOP_CUT = 5 (можно менять в constants.py) документы, отранжированные по tf, хранящейся в индексе.
Проверить правильность ответа можно перейдя по ссылке на Википедию (она содержится в ответе), и Ctrl+F по секции Plot. Количество упоминаний в документе (tf) можно сравнивать по ключам в .json файлах.

**5. Построение эмбеддингов.**

Для построения эмбеддингов необходимо запустить файл bert_embeddings.py. Эмбеддинги строятся на текстах Plot с max_len=512 (ограничение BERT) при помощи DistilBERT по батчам 100 doc_id, 
хранятся в общем смердженном result.csv файле в директории ata/bert_processed.
Далее реализованы ранжирование при помощи сравнения косинусной меры между эмбеддингами (эмбеддинг запроса строится на основе всех слов, входящих в запрос, за исключением keywords)

**6. Исправление опечаток при вводе запроса.**

Исправление осуществляется при помщи метода get_spelling_corrected_query, встроено в query_processing.py.
Реализовано при помощи deeppavlov spelling corrector. Однако, стоит отметить, что слияние слов неприминимо, поскольку всё ещё остаётся ограничение на запрос в виде токен-ключевое слово-токен

**Проверка работы**:

1. Запуск **build_index.py** для построения инвертированного индекса

2. Запуск **query_processing.py** с введением запроса в консоли, соответствующем формату.

**Примеры запросов, валидные формату** (слово - операция - слово - операция - слово и тд):

cska and coach and 1956 ---> Legend №17

cska and coach ----> Legend №17 and Going Vertical

harry and potter andnot chamber  ----> HP films except "Chamber of Secrets"

Voldemort or Harry and Potter

cska and coach or soviet 

cska andnot spartak

cska ornot spain

Voldemort  ----> HP films

simple NOT can be checked with 1st term being invalid and second valid with ornot:
bhbhkbu ornot cska

check amountof documents returned:
study 1133
work 7759
study and work 356
study or work 8536 = 1133-356+7759


**Проверка 2й части задания**:

stuudy and wark -> study and work-> 356 and ranked by cosine similarity

error and eexit -> exit and error-> 4 and ranked by cosine similarity

Сортировка за счёт косинусной меры эмбеддингов уменьшает смещение ранжирования в сторону фильмов с более длинным описанием сюжета, так как tf терма в этом случае имеет большее значение.
Также выводится топ 5 значений.