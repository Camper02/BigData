import csv
from collections import defaultdict


class MapReduce:
    def __init__(self):
        self.intermediate = defaultdict(list)
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value)

    def execute(self, data, mapper, reducer):
        # Фаза Map
        for record in data:
            mapper(record)

        # Фаза Shuffle & Sort
        sorted_intermediate = sorted(self.intermediate.items())

        # Фаза Reduce
        for key, values in sorted_intermediate:
            reducer(key, values)

        return self.result


def mapper(record):
    try:
        username = record['reviews.username']
        if username and username != '':  # Проверяем, что username не пустой
            helpful = int(record['reviews.numHelpful']) if record['reviews.numHelpful'] else 0
            rating = float(record['reviews.rating']) if record['reviews.rating'] else 0

            # Emit: (username, (helpful, rating, 1))
            mr.emit_intermediate(username, (helpful, rating, 1))
    except (ValueError, KeyError) as e:
        # Пропускаем записи с ошибками
        pass


def reducer(key, values):
    total_helpful = 0
    total_rating = 0
    count = 0

    for value in values:
        helpful, rating, cnt = value
        total_helpful += helpful
        total_rating += rating
        count += cnt

    if count > 0:
        avg_rating = total_rating / count
        # Emit: (username, total_helpful, avg_rating, count)
        mr.emit((key, total_helpful, round(avg_rating, 2), count))


# Основная программа
if __name__ == "__main__":
    # Чтение данных из CSV файла
    data = []
    with open('1429_1.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    # Создаем экземпляр MapReduce
    mr = MapReduce()

    # Запускаем MapReduce
    results = mr.execute(data, mapper, reducer)

    # Сортируем результаты по убыванию полезности
    sorted_results = sorted(results, key=lambda x: x[1], reverse=True)

    # Выводим топ-5 результатов
    print("Топ-5 пользователей по полезности отзывов:")
    print("=" * 70)
    print(f"{'Username':<20} {'Total Helpful':<15} {'Avg Rating':<12} {'Reviews Count':<15}")
    print("-" * 70)

    for i, (username, helpful, avg_rating, count) in enumerate(sorted_results[:5], 1):
        print(f"{i}. {username:<18} {helpful:<14} {avg_rating:<11} {count:<14}")

    # Дополнительная статистика
    print("\n" + "=" * 70)
    print("Дополнительная статистика:")
    print(f"Всего пользователей: {len(sorted_results)}")
    print(f"Всего отзывов обработано: {len(data)}")

    # Анализ распределения полезности
    helpful_distribution = defaultdict(int)
    for _, helpful, _, _ in sorted_results:
        if helpful == 0:
            helpful_distribution['0'] += 1
        elif helpful == 1:
            helpful_distribution['1'] += 1
        else:
            helpful_distribution['2+'] += 1

    print("\nРаспределение пользователей по полезности отзывов:")
    for category, count in helpful_distribution.items():
        print(f"  Полезность {category}: {count} пользователей")