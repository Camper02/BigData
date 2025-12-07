import networkx as nx
from database import Database
import config


class PregelPageRank:
    def __init__(self, db):
        self.db = db
        self.damping = config.DAMPING_FACTOR
        self.graph = nx.DiGraph()

    def build_graph(self):
        """Строим граф из данных БД"""
        documents = self.db.get_all_documents()
        links = self.db.get_all_links()

        # Добавляем узлы (документы)
        for doc in documents:
            self.graph.add_node(doc.id, pagerank=1.0 / len(documents))

        # Добавляем ребра (ссылки)
        for link in links:
            self.graph.add_edge(link.source_id, link.target_id)

        print(f"Построен граф: {len(documents)} узлов, {len(links)} ребер")

    def pregel_iteration(self):
        """Одна итерация в стиле Pregel"""
        new_pagerank = {}
        N = len(self.graph.nodes())

        # Вычисляем сумму PageRank для узлов без исходящих ребер
        sink_pr = 0
        for node in self.graph.nodes():
            if self.graph.out_degree[node] == 0:
                sink_pr += self.graph.nodes[node]['pagerank']

        for node in self.graph.nodes():
            # Начальное значение
            new_pr = (1 - self.damping) / N

            # Вклад от узлов без исходящих ссылок
            new_pr += self.damping * sink_pr / N

            # Вклад от входящих ссылок
            for predecessor in self.graph.predecessors(node):
                outgoing_count = self.graph.out_degree[predecessor]
                if outgoing_count > 0:
                    new_pr += self.damping * (
                            self.graph.nodes[predecessor]['pagerank'] / outgoing_count
                    )

            new_pagerank[node] = new_pr

        # ОШИБКА ЗДЕСЬ: Сначала нужно вычислить max_change, потом обновлять!
        max_change = 0
        for node, pr in new_pagerank.items():
            old_pr = self.graph.nodes[node]['pagerank']
            change = abs(pr - old_pr)
            if change > max_change:
                max_change = change
            self.graph.nodes[node]['pagerank'] = pr  # Обновляем ПОСЛЕ вычисления

        return max_change

    def calculate_pagerank(self, max_iterations=config.MAX_ITERATIONS):
        """Вычисление PageRank в стиле Pregel"""
        self.build_graph()

        print("Запуск PageRank через Pregel-подобный алгоритм...")

        for i in range(max_iterations):
            max_change = self.pregel_iteration()

            print(f"Итерация {i + 1}: максимальное изменение = {max_change:.6f}")

            if max_change < config.TOLERANCE:
                print(f"Сходимость достигнута на итерации {i + 1}")
                break

        # Обновляем значения в БД
        for node in self.graph.nodes():
            self.db.update_pagerank(node, self.graph.nodes[node]['pagerank'])

        # Возвращаем результаты
        return {node: self.graph.nodes[node]['pagerank'] for node in self.graph.nodes()}