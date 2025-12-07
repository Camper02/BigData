from database import Database
import math
from collections import defaultdict


class SearchEngine:
    def __init__(self, db):
        self.db = db
        self.doc_count = len(self.db.get_all_documents())
        self.avg_doc_length = self._calculate_avg_doc_length()

    def _calculate_avg_doc_length(self):
        """Вычисление средней длины документа"""
        total_length = 0
        docs = self.db.get_all_documents()
        for doc in docs:
            if doc.content:
                total_length += len(doc.content.split())
        return total_length / self.doc_count if self.doc_count > 0 else 1

    def document_at_a_time(self, query, k=5):
        """
        Document-at-a-time подход
        Обрабатываем все документы одновременно для каждого терма запроса
        """
        query_terms = self._tokenize_query(query)
        scores = defaultdict(float)

        # Получаем все документы
        all_docs = self.db.get_all_documents()

        # Для каждого документа вычисляем релевантность
        for doc in all_docs:
            if not doc.content:
                continue

            doc_score = 0
            doc_terms = [term.word for term in doc.terms]
            doc_length = len(doc.content.split())

            for term in query_terms:
                # TF (частота термина в документе)
                tf = doc_terms.count(term)
                if tf == 0:
                    continue

                # IDF (обратная частота документа)
                term_docs = self.db.search_by_term(term)
                idf = math.log((self.doc_count + 1) / (len(term_docs) + 0.5))

                # BM25 scoring
                k1 = 1.2
                b = 0.75
                tf_component = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / self.avg_doc_length)))

                doc_score += idf * tf_component

            # Учитываем PageRank
            final_score = doc_score * (1 + math.log1p(doc.pagerank))
            scores[doc.id] = final_score

        # Сортировка результатов
        sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:k]

        # Получаем документы
        results = []
        for doc_id, score in sorted_results:
            doc = next((d for d in all_docs if d.id == doc_id), None)
            if doc:
                results.append({
                    'title': doc.title,
                    'url': doc.url,
                    'score': score,
                    'pagerank': doc.pagerank,
                    'snippet': self._create_snippet(doc.content, query_terms)
                })

        return results

    def term_at_a_time(self, query, k=5):
        """
        Term-at-a-time подход
        Обрабатываем все документы для каждого терма по отдельности
        """
        query_terms = self._tokenize_query(query)
        scores = defaultdict(float)

        # Создаем инвертированный индекс
        inverted_index = {}
        for term in query_terms:
            term_docs = self.db.search_by_term(term)
            if term_docs:
                inverted_index[term] = term_docs

        # Для каждого терма вычисляем вклад в документы
        for term, docs in inverted_index.items():
            # IDF для терма
            idf = math.log((self.doc_count + 1) / (len(docs) + 0.5))

            for doc in docs:
                # TF для документа
                doc_terms = [t.word for t in doc.terms]
                tf = doc_terms.count(term)

                if tf > 0:
                    doc_length = len(doc.content.split())

                    # BM25 scoring для этого терма
                    k1 = 1.2
                    b = 0.75
                    tf_component = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / self.avg_doc_length)))

                    # Добавляем вклад терма к документу
                    scores[doc.id] += idf * tf_component

        # Учитываем PageRank и сортируем
        all_docs = self.db.get_all_documents()
        for doc_id in scores:
            doc = next((d for d in all_docs if d.id == doc_id), None)
            if doc:
                scores[doc_id] *= (1 + math.log1p(doc.pagerank))

        sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:k]

        # Формируем результаты
        results = []
        for doc_id, score in sorted_results:
            doc = next((d for d in all_docs if d.id == doc_id), None)
            if doc:
                results.append({
                    'title': doc.title,
                    'url': doc.url,
                    'score': score,
                    'pagerank': doc.pagerank,
                    'snippet': self._create_snippet(doc.content, query_terms)
                })

        return results

    def _tokenize_query(self, query):
        """Токенизация запроса"""
        import re
        words = re.findall(r'\b[а-яa-z]{3,}\b', query.lower())
        stop_words = {'и', 'в', 'с', 'по', 'на', 'не', 'что', 'это', 'как', 'а', 'но', 'или'}
        return [word for word in words if word not in stop_words]

    def _create_snippet(self, content, query_terms, max_length=150):
        """Создание сниппета с выделением найденных терминов"""
        if not content:
            return ""

        # Находим первое вхождение любого из терминов
        content_lower = content.lower()
        first_pos = len(content)

        for term in query_terms:
            pos = content_lower.find(term)
            if pos != -1 and pos < first_pos:
                first_pos = pos

        # Берем фрагмент вокруг найденного термина
        start = max(0, first_pos - 50)
        end = min(len(content), start + max_length)

        snippet = content[start:end]

        if start > 0:
            snippet = "..." + snippet
        if end < len(content):
            snippet = snippet + "..."

        # Выделяем термины запроса
        for term in query_terms:
            snippet = snippet.replace(term, f"**{term}**")
            snippet = snippet.replace(term.capitalize(), f"**{term.capitalize()}**")

        return snippet

    def hybrid_search(self, query, k=5, alpha=0.7):
        """
        Гибридный поиск: комбинация document-at-a-time и term-at-a-time
        alpha - вес document-at-a-time подхода
        """
        daat_results = self.document_at_a_time(query, k * 2)
        taat_results = self.term_at_a_time(query, k * 2)

        # Нормализуем scores
        if daat_results:
            max_daat = max(r['score'] for r in daat_results)
            for r in daat_results:
                r['normalized_score'] = r['score'] / max_daat if max_daat > 0 else 0

        if taat_results:
            max_taat = max(r['score'] for r in taat_results)
            for r in taat_results:
                r['normalized_score'] = r['score'] / max_taat if max_taat > 0 else 0

        # Объединяем результаты
        combined = {}
        for r in daat_results:
            combined[r['url']] = {
                'data': r,
                'score': r['normalized_score'] * alpha
            }

        for r in taat_results:
            if r['url'] in combined:
                combined[r['url']]['score'] += r['normalized_score'] * (1 - alpha)
            else:
                combined[r['url']] = {
                    'data': r,
                    'score': r['normalized_score'] * (1 - alpha)
                }

        # Сортируем по комбинированному score
        sorted_results = sorted(
            combined.items(),
            key=lambda x: x[1]['score'],
            reverse=True
        )[:k]

        return [item[1]['data'] for item in sorted_results]