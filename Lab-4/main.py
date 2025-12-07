from database import Database, Document, Term, Link
from parser import Parser
from pagerank_mr import MapReducePageRank
from pagerank_pregel import PregelPageRank
from search_engine import SearchEngine
import config
import warnings
from sqlalchemy import exc as sa_exc

# Игнорировать предупреждения SQLAlchemy
warnings.filterwarnings('ignore', category=sa_exc.SAWarning)

def main():
    print("=" * 60)
    print("МИНИ-ПОИСКОВИК: ПОЛНАЯ ДЕМОНСТРАЦИЯ")
    print("=" * 60)

    # Инициализация БД
    print("\n1. ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ")
    db = Database()

    # Парсинг документов
    print("\n2. ПАРСИНГ ДОКУМЕНТОВ")
    parser = Parser(db)
    documents = parser.parse_directory(config.DATA_DIR)


    # Теперь проверьте PageRank
    links_count = db.session.query(Link).count()
    print(f"Всего ссылок в БД: {links_count}")

    # Вывод статистики
    print(f"\nСтатистика базы данных:")
    print(f"- Документов: {len(db.get_all_documents())}")
    print(f"- Уникальных слов: {db.session.query(Term).count()}")
    print(f"- Ссылок: {db.session.query(Link).count()}")

    # PageRank через MapReduce
    print("\n3. ВЫЧИСЛЕНИЕ PAGERANK (MAPREDUCE)")
    mr_pr = MapReducePageRank(config.DB_PATH)
    mr_results = mr_pr.calculate_pagerank(iterations=20)

    print("\nРезультаты PageRank (MapReduce):")
    for doc_id, pr_value in sorted(mr_results.items(), key=lambda x: x[1], reverse=True):
        doc = db.session.query(Document).get(doc_id)
        if doc:
            print(f"  {doc.title}: {pr_value:.6f}")

    print("\n=== ДИАГНОСТИКА ПЕРЕД PAGERANK ===")

    # PageRank через Pregel
    print("\n4. ВЫЧИСЛЕНИЕ PAGERANK (PREGEL)")
    pregel_pr = PregelPageRank(db)
    pregel_results = pregel_pr.calculate_pagerank(max_iterations=20)

    print("\nРезультаты PageRank (Pregel):")
    for doc_id, pr_value in sorted(pregel_results.items(), key=lambda x: x[1], reverse=True):
        doc = db.session.query(Document).get(doc_id)
        if doc:
            print(f"  {doc.title}: {pr_value:.6f}")

    # Поиск
    print("\n5. ПОЛНОТЕКСТОВЫЙ ПОИСК")
    search_engine = SearchEngine(db)

    # Тестовые запросы
    test_queries = [
        "машинное обучение",
        "нейронные сети алгоритмы",
        "обработка естественного языка",
        "графы PageRank"
    ]

    for query in test_queries:
        print(f"\nЗапрос: '{query}'")
        print("-" * 40)

        print("\na) Document-at-a-time подход:")
        daat_results = search_engine.document_at_a_time(query, k=3)
        for i, result in enumerate(daat_results, 1):
            print(f"  {i}. {result['title']}")
            print(f"     Score: {result['score']:.4f}, PageRank: {result['pagerank']:.6f}")
            print(f"     Сниппет: {result['snippet']}")

        print("\nb) Term-at-a-time подход:")
        taat_results = search_engine.term_at_a_time(query, k=3)
        for i, result in enumerate(taat_results, 1):
            print(f"  {i}. {result['title']}")
            print(f"     Score: {result['score']:.4f}, PageRank: {result['pagerank']:.6f}")
            print(f"     Сниппет: {result['snippet']}")

        print("\nc) Гибридный поиск:")
        hybrid_results = search_engine.hybrid_search(query, k=3)
        for i, result in enumerate(hybrid_results, 1):
            print(f"  {i}. {result['title']}")
            print(f"     Score: {result['score']:.4f}, PageRank: {result['pagerank']:.6f}")
            print(f"     Сниппет: {result['snippet']}")

    # Интерактивный поиск
    print("\n" + "=" * 60)
    print("ИНТЕРАКТИВНЫЙ ПОИСК")
    print("=" * 60)

    while True:
        query = input("\nВведите поисковый запрос (или 'quit' для выхода): ")
        if query.lower() == 'quit':
            break

        print(f"\nРезультаты поиска для: '{query}'")
        print("-" * 40)

        results = search_engine.hybrid_search(query, k=5)

        if not results:
            print("По вашему запросу ничего не найдено.")
            continue

        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['title']}")
            print(f"   URL: {result['url']}")
            print(f"   Релевантность: {result['score']:.4f}")
            print(f"   PageRank: {result['pagerank']:.6f}")
            print(f"   {result['snippet']}")

    db.close()
    print("\nРабота завершена!")


if __name__ == "__main__":
    main()