import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin, urlparse
import time


class RealSiteDownloader:
    def __init__(self, output_dir="data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def download_site(self, url, filename):
        """Скачивание страницы и сохранение локально"""
        try:
            print(f"Скачиваю: {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Преобразуем абсолютные ссылки для локального использования
            for tag in soup.find_all(['a', 'img', 'link', 'script']):
                for attr in ['href', 'src']:
                    if tag.get(attr):
                        absolute_url = urljoin(url, tag[attr])
                        # Для ссылок на тот же домен делаем относительные
                        if self.same_domain(url, absolute_url):
                            tag[attr] = os.path.basename(urlparse(absolute_url).path) or 'index.html'

            # Сохраняем
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(str(soup))

            print(f"Сохранено: {filename}")
            return True

        except Exception as e:
            print(f"Ошибка при скачивании {url}: {e}")
            return False

    def same_domain(self, url1, url2):
        """Проверка, что ссылки ведут на тот же домен"""
        return urlparse(url1).netloc == urlparse(url2).netloc

    def download_all(self):
        """Скачиваем набор реальных сайтов"""

        # 1. Habr - IT статьи с перекрестными ссылками
        habr_sites = [
            ("https://habr.com/ru/articles/448276/", "site1.html"),  # PageRank
            ("https://habr.com/ru/articles/149693/", "site2.html"),  # MapReduce
        ]

        # 2. Wikipedia - научные статьи
        wiki_sites = [
            ("https://ru.wikipedia.org/wiki/PageRank", "site3.html"),
            ("https://ru.wikipedia.org/wiki/MapReduce", "site4.html"),
        ]


        all_sites = habr_sites + wiki_sites

        downloaded = []
        for url, filename in all_sites:
            if self.download_site(url, filename):
                downloaded.append(filename)
            time.sleep(2)  # Задержка между запросами

        print(f"\nСкачано {len(downloaded)} из {len(all_sites)} сайтов")
        return downloaded


# Альтернативный вариант - создаем упрощенные HTML с реальным контентом
def create_realistic_sites():
    """Создаем HTML файлы с реальным контентом (если скачивание не работает)"""

    sites = {
        "habr_ml.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>Машинное обучение на Хабре</title>
</head>
<body>
    <h1>Машинное обучение: от теории к практике</h1>
    <p>Машинное обучение (Machine Learning) — это подраздел искусственного интеллекта, изучающий методы построения алгоритмов, способных обучаться.</p>
    <p>Основные типы ML: обучение с учителем, без учителя, с подкреплением.</p>

    <h2>Ссылки на связанные статьи</h2>
    <ul>
        <li><a href="habr_nn.html">Нейронные сети для начинающих</a></li>
        <li><a href="habr_pagerank.html">Алгоритм PageRank от Google</a></li>
        <li><a href="habr_mapreduce.html">MapReduce для обработки больших данных</a></li>
    </ul>

    <h2>Популярные алгоритмы</h2>
    <ul>
        <li>Линейная регрессия</li>
        <li>Логистическая регрессия</li>
        <li>Деревья решений</li>
        <li>Метод опорных векторов (SVM)</li>
        <li>Алгоритм k-ближайших соседей</li>
    </ul>

    <footer>
        <p>Источник: <a href="https://habr.com">Habr.com</a></p>
    </footer>
</body>
</html>
""",

        "habr_nn.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>Нейронные сети на Хабре</title>
</head>
<body>
    <h1>Нейронные сети: архитектуры и применение</h1>
    <p>Нейронные сети — это вычислительные системы, вдохновленные биологическими нейронными сетями мозга.</p>

    <h2>Типы нейронных сетей</h2>
    <ul>
        <li>Полносвязные сети (Fully Connected)</li>
        <li>Сверточные нейронные сети (CNN) для изображений</li>
        <li>Рекуррентные нейронные сети (RNN) для последовательностей</li>
        <li>Генеративно-состязательные сети (GAN)</li>
        <li>Трансформеры для NLP</li>
    </ul>

    <h2>Ссылки на связанные статьи</h2>
    <ul>
        <li><a href="habr_ml.html">Основы машинного обучения</a></li>
        <li><a href="wiki_nlp.html">Обработка естественного языка</a></li>
        <li><a href="habr_pagerank.html">Алгоритмы ранжирования</a></li>
    </ul>

    <h2>Фреймворки</h2>
    <p>TensorFlow, PyTorch, Keras — популярные фреймворки для глубокого обучения.</p>

    <footer>
        <p>Источник: <a href="https://habr.com">Habr.com</a></p>
    </footer>
</body>
</html>
""",

        "wiki_ml.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>Машинное обучение — Википедия</title>
</head>
<body>
    <h1>Машинное обучение</h1>

    <div class="infobox">
        <p><strong>Машинное обучение</strong> (англ. machine learning) — раздел искусственного интеллекта.</p>
    </div>

    <h2>Содержание</h2>
    <ol>
        <li><a href="#definition">Определение</a></li>
        <li><a href="#types">Типы обучения</a></li>
        <li><a href="#algorithms">Алгоритмы</a></li>
        <li><a href="#applications">Применение</a></li>
    </ol>

    <h3 id="definition">Определение</h3>
    <p>Машинное обучение — класс методов искусственного интеллекта, характерной чертой которых является не прямое решение задачи, а обучение в процессе применения решений множества сходных задач.</p>

    <h3 id="types">Типы обучения</h3>
    <ul>
        <li><strong>Обучение с учителем</strong> (Supervised learning)</li>
        <li><strong>Обучение без учителя</strong> (Unsupervised learning)</li>
        <li><strong>Обучение с подкреплением</strong> (Reinforcement learning)</li>
        <li><strong>Самообучение</strong> (Self-supervised learning)</li>
    </ul>

    <h3 id="algorithms">Алгоритмы</h3>
    <p>Популярные алгоритмы машинного обучения включают:</p>
    <ul>
        <li>Линейная регрессия</li>
        <li>Логистическая регрессия</li>
        <li>Деревья решений</li>
        <li>Случайный лес</li>
        <li>Метод опорных векторов</li>
        <li>Нейронные сети</li>
    </ul>

    <h3 id="applications">Применение</h3>
    <p>ML применяется в распознавании образов, компьютерном зрении, обработке естественного языка, биоинформатике и других областях.</p>

    <h2>См. также</h2>
    <ul>
        <li><a href="wiki_nn.html">Нейронные сети</a></li>
        <li><a href="wiki_nlp.html">Обработка естественного языка</a></li>
        <li><a href="wiki_pagerank.html">PageRank</a></li>
        <li><a href="habr_ml.html">Практические статьи на Хабре</a></li>
    </ul>

    <footer>
        <p>Материал из Википедии — свободной энциклопедии</p>
    </footer>
</body>
</html>
""",

        "wiki_pagerank.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>PageRank — Википедия</title>
</head>
<body>
    <h1>PageRank</h1>

    <div class="infobox">
        <p><strong>PageRank</strong> — алгоритм расчёта авторитетности страницы, используемый поисковой системой Google.</p>
    </div>

    <h2>История</h2>
    <p>Алгоритм был разработан основателями Google Ларри Пейджем и Сергеем Брином в 1998 году.</p>

    <h2>Математическая формула</h2>
    <p>PageRank вычисляется по формуле:</p>
    <p>PR(A) = (1-d) + d * (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))</p>

    <h2>Применение</h2>
    <ul>
        <li>Ранжирование веб-страниц в поисковиках</li>
        <li>Анализ социальных сетей</li>
        <li>Научные исследования цитирования</li>
    </ul>

    <h2>См. также</h2>
    <ul>
        <li><a href="wiki_ml.html">Машинное обучение</a></li>
        <li><a href="habr_pagerank.html">PageRank на Хабре</a></li>
        <li><a href="wiki_mapreduce.html">MapReduce для распределенных вычислений</a></li>
    </ul>

    <h2>Внешние ссылки</h2>
    <ul>
        <li><a href="habr_pagerank.html">Практическая реализация PageRank</a></li>
    </ul>

    <footer>
        <p>Материал из Википедии — свободной энциклопедии</p>
    </footer>
</body>
</html>
""",

        "habr_pagerank.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>PageRank на Хабре</title>
</head>
<body>
    <h1>Алгоритм PageRank: от теории к реализации</h1>

    <p>PageRank — вероятностный алгоритм, лежащий в основе ранжирования Google.</p>

    <h2>Основные концепции</h2>
    <ul>
        <li>Вероятность перехода пользователя по ссылке</li>
        <li>Коэффициент затухания (damping factor)</li>
        <li>Итеративный процесс вычисления</li>
        <li>Сходимость алгоритма</li>
    </ul>

    <h2>Реализация на Python</h2>
    <pre><code>
def pagerank(graph, d=0.85, iterations=100):
    N = len(graph)
    pr = dict.fromkeys(graph.keys(), 1/N)

    for _ in range(iterations):
        new_pr = {}
        for node in graph:
            rank = (1-d)/N
            rank += d * sum(pr[inlink]/len(graph[inlink]) 
                           for inlink in get_inlinks(node))
            new_pr[node] = rank
        pr = new_pr

    return pr
    </code></pre>

    <h2>Ссылки на связанные темы</h2>
    <ul>
        <li><a href="wiki_pagerank.html">Теоретические основы PageRank</a></li>
        <li><a href="habr_mapreduce.html">Реализация через MapReduce</a></li>
        <li><a href="wiki_ml.html">Машинное обучение для ранжирования</a></li>
    </ul>

    <footer>
        <p>Источник: <a href="https://habr.com">Habr.com</a></p>
    </footer>
</body>
</html>
""",

        "habr_mapreduce.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>MapReduce на Хабре</title>
</head>
<body>
    <h1>MapReduce: парадигма распределенных вычислений</h1>

    <p>MapReduce — программная модель для обработки больших данных в распределенных кластерах.</p>

    <h2>Основные этапы</h2>
    <ol>
        <li><strong>Map</strong>: Преобразование входных данных в пары ключ-значение</li>
        <li><strong>Shuffle</strong>: Группировка значений по ключам</li>
        <li><strong>Reduce</strong>: Агрегация значений для каждого ключа</li>
    </ol>

    <h2>Пример: подсчет слов</h2>
    <pre><code>
# Map
def map_function(document):
    for word in document.split():
        yield (word, 1)

# Reduce  
def reduce_function(word, counts):
    yield (word, sum(counts))
    </code></pre>

    <h2>Применение для PageRank</h2>
    <p>MapReduce идеально подходит для итеративного вычисления PageRank в распределенных системах.</p>

    <h2>Ссылки</h2>
    <ul>
        <li><a href="habr_pagerank.html">PageRank алгоритм</a></li>
        <li><a href="wiki_mapreduce.html">Теория MapReduce</a></li>
        <li><a href="habr_ml.html">Обработка больших данных в ML</a></li>
    </ul>

    <footer>
        <p>Источник: <a href="https://habr.com">Habr.com</a></p>
    </footer>
</body>
</html>
""",

        "wiki_nlp.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>Обработка естественного языка — Википедия</title>
</head>
<body>
    <h1>Обработка естественного языка</h1>

    <p><strong>Обработка естественного языка</strong> (Natural Language Processing, NLP) — направление искусственного интеллекта и математической лингвистики.</p>

    <h2>Задачи NLP</h2>
    <ul>
        <li>Морфологический анализ</li>
        <li>Синтаксический анализ</li>
        <li>Семантический анализ</li>
        <li>Прагматический анализ</li>
    </ul>

    <h2>Методы</h2>
    <ul>
        <li>Статистические методы</li>
        <li>Нейронные сети</li>
        <li>Трансформеры</li>
        <li>Языковые модели</li>
    </ul>

    <h2>См. также</h2>
    <ul>
        <li><a href="wiki_ml.html">Машинное обучение</a></li>
        <li><a href="wiki_nn.html">Нейронные сети</a></li>
        <li><a href="habr_nn.html">Практика NLP на Хабре</a></li>
    </ul>

    <footer>
        <p>Материал из Википедии — свободной энциклопедии</p>
    </footer>
</body>
</html>
""",

        "wiki_mapreduce.html": """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>MapReduce — Википедия</title>
</head>
<body>
    <h1>MapReduce</h1>

    <p><strong>MapReduce</strong> — модель распределённых вычислений, представленная компанией Google.</p>

    <h2>Архитектура</h2>
    <ul>
        <li>Master node</li>
        <li>Worker nodes</li>
        <li>Input/Output системы</li>
    </ul>

    <h2>Применение</h2>
    <ul>
        <li>Обработка логов</li>
        <li>Индексация веб-страниц</li>
        <li>Машинное обучение в больших масштабах</li>
    </ul>

    <h2>Реализации</h2>
    <ul>
        <li>Apache Hadoop</li>
        <li>Apache Spark</li>
        <li>Google Cloud Dataflow</li>
    </ul>

    <h2>См. также</h2>
    <ul>
        <li><a href="habr_mapreduce.html">Практическое применение MapReduce</a></li>
        <li><a href="wiki_pagerank.html">PageRank для распределенных вычислений</a></li>
    </ul>

    <footer>
        <p>Материал из Википедии — свободной энциклопедии</p>
    </footer>
</body>
</html>
"""
    }

    for filename, content in sites.items():
        filepath = os.path.join("data", filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Создан: {filename}")

    return list(sites.keys())


if __name__ == "__main__":
    print("Скачиваем реальные сайты...")

    # Способ 1: Попробуем скачать реальные сайты
    downloader = RealSiteDownloader()
    try:
        downloaded = downloader.download_all()
    except:
        print("Не удалось скачать сайты. Создаем локальные копии...")
        # Способ 2: Создаем HTML файлы локально
        downloaded = create_realistic_sites()