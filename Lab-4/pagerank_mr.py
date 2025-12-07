import sqlite3
from collections import defaultdict
import config


class MapReducePageRank:
    def __init__(self, db_path):
        self.db_path = db_path
        self.damping = config.DAMPING_FACTOR

    def map_links(self):
        """Map: собираем исходящие ссылки для каждого документа"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Получаем все документы
        cursor.execute("SELECT id FROM documents")
        doc_ids = [row[0] for row in cursor.fetchall()]

        # Собираем исходящие ссылки
        outgoing = defaultdict(list)
        cursor.execute("SELECT source_id, target_id FROM links")
        for source_id, target_id in cursor.fetchall():
            outgoing[source_id].append(target_id)

        conn.close()
        return doc_ids, outgoing

    def reduce_pagerank(self, doc_ids, outgoing, current_pr):
        """Reduce: вычисляем новый PageRank для каждого документа"""
        N = len(doc_ids)
        new_pr = {}

        # Вычисляем сумму PageRank для документов без исходящих ссылок
        sink_pr = 0
        for doc_id in doc_ids:
            if doc_id not in outgoing or not outgoing[doc_id]:
                sink_pr += current_pr.get(doc_id, 1.0 / N)

        for doc_id in doc_ids:
            # Часть от случайного перехода
            random_walk = (1 - self.damping) / N

            # Часть от суммы PageRank документов без ссылок
            sink_contribution = self.damping * sink_pr / N

            # Часть от входящих ссылок
            incoming_contribution = 0

            # Находим все документы, ссылающиеся на текущий
            for source_id in outgoing:
                if doc_id in outgoing[source_id]:
                    incoming_contribution += current_pr.get(source_id, 1.0 / N) / len(outgoing[source_id])

            incoming_contribution = self.damping * incoming_contribution

            new_pr[doc_id] = random_walk + sink_contribution + incoming_contribution

        return new_pr

    def calculate_pagerank(self, iterations=config.MAX_ITERATIONS):
        """Основной алгоритм PageRank через MapReduce"""
        # Инициализация
        doc_ids, outgoing = self.map_links()
        N = len(doc_ids)
        pagerank = {doc_id: 1.0 / N for doc_id in doc_ids}

        print("Запуск PageRank через MapReduce...")

        for i in range(iterations):
            new_pagerank = self.reduce_pagerank(doc_ids, outgoing, pagerank)

            # Проверка сходимости
            diff = sum(abs(new_pagerank[doc_id] - pagerank[doc_id]) for doc_id in doc_ids)

            pagerank = new_pagerank

            print(f"Итерация {i + 1}: максимальное изменение = {diff:.6f}")

            if diff < config.TOLERANCE:
                print(f"Сходимость достигнута на итерации {i + 1}")
                break

        # Обновляем значения в БД
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for doc_id, pr_value in pagerank.items():
            cursor.execute(
                "UPDATE documents SET pagerank = ? WHERE id = ?",
                (pr_value, doc_id)
            )

        conn.commit()
        conn.close()

        return pagerank