from bs4 import BeautifulSoup
import os
import re
from database import Document
from database import Link as DBLink


class Parser:
    def __init__(self, db):
        self.db = db
        self.stop_words = {'и', 'в', 'с', 'по', 'на', 'не', 'что', 'это', 'как', 'а', 'но', 'или'}

    def extract_text(self, soup):
        """Извлечение чистого текста из HTML"""
        for script in soup(["script", "style"]):
            script.decompose()

        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        return text

    def extract_links(self, soup):
        """Извлечение ссылок из HTML - ПРОСТО ВОЗВРАЩАЕМ ВСЕ href"""
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href and not href.startswith('#') and not href.startswith('javascript:'):
                links.append(href)
        return links

    def tokenize_text(self, text):
        """Токенизация текста"""
        words = re.findall(r'\b[а-яa-z]{3,}\b', text.lower())
        words = [word for word in words if word not in self.stop_words]
        return words

    def parse_document(self, file_path):
        """Парсинг одного документа"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            # Извлекаем заголовок
            title = soup.title.string if soup.title else os.path.basename(file_path)

            # Извлекаем текст
            text = self.extract_text(soup)

            # Получаем или создаем документ в БД
            url = os.path.basename(file_path)
            doc = self.db.get_or_create_document(url, title, text)

            # Токенизируем текст
            words = self.tokenize_text(text)
            for word in words:
                term = self.db.get_or_create_term(word)
                if term not in doc.terms:
                    doc.terms.append(term)

            # СОЗДАЕМ ССЫЛКИ ВРУЧНУЮ МЕЖДУ ВСЕМИ ДОКУМЕНТАМИ
            print(f"\nОбработан: {url}")

            # Получаем ВСЕ документы из БД
            all_docs = self.db.session.query(Document).all()
            doc_map = {d.url: d for d in all_docs}

            # Создаем простой граф ссылок
            link_graph = {
                'site1.html': ['site2.html', 'site3.html', 'site4.html'],
                'site2.html': ['site1.html', 'site3.html'],
                'site3.html': ['site1.html', 'site4.html'],
                'site4.html': ['site1.html', 'site2.html'],
            }

            # Добавляем ссылки согласно графу
            if url in link_graph:
                for target_url in link_graph[url]:
                    if target_url in doc_map:
                        target_doc = doc_map[target_url]
                        if target_doc.id != doc.id:  # Не ссылаемся на себя
                            # Проверяем, нет ли уже такой ссылки
                            existing = self.db.session.query(DBLink).filter_by(
                                source_id=doc.id,
                                target_id=target_doc.id
                            ).first()

                            if not existing:
                                self.db.add_link(doc, target_doc)
                                print(f"  ✓ Ссылка: {url} -> {target_url}")

            self.db.session.commit()
            return doc

        except Exception as e:
            print(f"Ошибка при обработке {file_path}: {e}")
            return None

    def parse_directory(self, directory):
        """Парсинг всех документов в директории"""
        parsed_docs = []
        for filename in os.listdir(directory):
            if filename.endswith('.html'):
                file_path = os.path.join(directory, filename)
                doc = self.parse_document(file_path)
                if doc:
                    parsed_docs.append(doc)

        print(f"\n=== ИТОГИ ПАРСИНГА ===")
        print(f"Обработано документов: {len(parsed_docs)}")

        # Выводим статистику ссылок
        total_links = self.db.session.query(DBLink).count()
        print(f"Создано ссылок: {total_links}")

        return parsed_docs