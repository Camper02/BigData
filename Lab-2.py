import numpy as np
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
        self.intermediate.clear()
        self.result.clear()

        # Фаза Map
        for record in data:
            mapper(self, record)  # Передаем self в mapper

        # Фаза Shuffle & Sort
        sorted_intermediate = sorted(self.intermediate.items())

        # Фаза Reduce
        for key, values in sorted_intermediate:
            reducer(self, key, values)  # Передаем self в reducer

        return self.result


class MatrixMultiplication:
    def __init__(self, A, B):
        self.A = A
        self.B = B
        self.m = len(A)
        self.n = len(A[0])
        self.p = len(B[0])

    def mapper_matrix(self, mr, record):
        matrix_name, idx1, idx2, value = record

        if matrix_name == 'A':
            # A[i][j] -> для всех k: emit((i,k), ('A', j, value))
            i, j = idx1, idx2
            for k in range(self.p): # (i,0), (i,1), ..., (i,p-1)
                mr.emit_intermediate((i, k), ('A', j, value))
        else:  # matrix_name == 'B'
            # B[j][k] -> для всех i: emit((i,k), ('B', j, value))
            j, k = idx1, idx2
            for i in range(self.m): # (0,k), (1,k), ..., (m-1,k)
                mr.emit_intermediate((i, k), ('B', j, value))

    def reducer_matrix(self, mr, key, values):
        i, k = key

        # Создаем массивы для значений A и B
        A_row = [0] * self.n
        B_col = [0] * self.n

        # Заполняем массивы
        for matrix_type, j, value in values:
            if matrix_type == 'A':
                A_row[j] = value
            else:
                B_col[j] = value

        # Вычисляем скалярное произведение
        result = 0
        for j in range(self.n):
            result += A_row[j] * B_col[j]

        mr.emit((i, k, result))

    def multiply(self):
        data = []

        # Матрица A: строки i, столбцы j
        for i in range(self.m):
            for j in range(self.n):
                data.append(('A', i, j, self.A[i][j]))

        # Матрица B: строки j, столбцы k
        for j in range(self.n):
            for k in range(self.p):
                data.append(('B', j, k, self.B[j][k]))

        mr = MapReduce()
        results = mr.execute(data, self.mapper_matrix, self.reducer_matrix)

        # Сортируем результаты по индексам
        results.sort(key=lambda x: (x[0], x[1]))

        # Формируем результирующую матрицу
        result_matrix = np.zeros((self.m, self.p))
        for i, k, value in results:
            if i < self.m and k < self.p:  # Проверка границ
                result_matrix[i][k] = value

        return result_matrix


class LinearRegression:
    def __init__(self, X, y):
        self.X = X  # Признаки
        self.y = y  # Целевая переменная
        self.n_samples = len(X)
        self.n_features = len(X[0])

    def mapper_regression(self, mr, record):
        sample_idx, x, y_val = record

        # Emit промежуточные значения для вычисления статистик
        # Для вычисления X^T * X
        for i in range(self.n_features):
            for j in range(self.n_features):
                mr.emit_intermediate(('XTX', i, j), x[i] * x[j])

        # Для вычисления X^T * y
        for i in range(self.n_features):
            mr.emit_intermediate(('XTy', i), x[i] * y_val)

        # Для вычисления сумм
        mr.emit_intermediate(('sum_x',), x)
        mr.emit_intermediate(('sum_y',), y_val)
        mr.emit_intermediate(('count',), 1)

    def reducer_regression(self, mr, key, values):
        if key[0] == 'XTX':
            # Суммируем элементы для X^T * X
            i, j = key[1], key[2]
            total = sum(values)
            mr.emit(('XTX_result', i, j, total))

        elif key[0] == 'XTy':
            # Суммируем элементы для X^T * y
            i = key[1]
            total = sum(values)
            mr.emit(('XTy_result', i, total))

        elif key[0] == 'sum_x':
            # Суммируем векторы x
            total = np.zeros(self.n_features)
            for vec in values:
                total += vec
            mr.emit(('sum_x_result', total))

        elif key[0] == 'sum_y':
            # Суммируем y
            total = sum(values)
            mr.emit(('sum_y_result', total))

        elif key[0] == 'count':
            # Подсчитываем количество samples
            total = sum(values)
            mr.emit(('count_result', total))

    def fit(self):
        # Подготавливаем данные
        data = []
        for idx, (x_vec, y_val) in enumerate(zip(self.X, self.y)):
            data.append((idx, x_vec, y_val))

        # Запускаем MapReduce для сбора статистик
        mr = MapReduce()
        results = mr.execute(data, self.mapper_regression, self.reducer_regression)

        # Извлекаем результаты из MapReduce
        XTX = np.zeros((self.n_features, self.n_features))
        XTy = np.zeros(self.n_features)
        sum_x = np.zeros(self.n_features)
        sum_y = 0
        count = 0

        for result in results:
            if result[0] == 'XTX_result':
                _, i, j, value = result
                XTX[i][j] = value
            elif result[0] == 'XTy_result':
                _, i, value = result
                XTy[i] = value
            elif result[0] == 'sum_x_result':
                sum_x = result[1]
            elif result[0] == 'sum_y_result':
                sum_y = result[1]
            elif result[0] == 'count_result':
                count = result[1]

        # Вычисляем коэффициенты регрессии: w = (X^T * X)^(-1) * X^T * y
        try:
            w = np.linalg.inv(XTX) @ XTy
        except np.linalg.LinAlgError:
            # Если матрица вырождена, используем псевдообратную
            w = np.linalg.pinv(XTX) @ XTy

        # Вычисляем intercept: b = mean(y) - w * mean(x)
        mean_x = sum_x / count
        mean_y = sum_y / count
        b = mean_y - np.dot(w, mean_x)

        return w, b

    def predict(self, X, w, b):
        return np.dot(X, w) + b


# Демонстрация работы
def demonstrate_matrix_multiplication():
    print("=" * 60)
    print("ДЕМОНСТРАЦИЯ: УМНОЖЕНИЕ МАТРИЦ")
    print("=" * 60)

    # Создаем тестовые матрицы
    A = np.array([
        [1, 2, 3],
        [4, 5, 6]
    ])

    B = np.array([
        [7, 8],
        [9, 10],
        [11, 12]
    ])

    print("Матрица A (2x3):")
    print(A)
    print("\nМатрица B (3x2):")
    print(B)

    # Умножение через MapReduce
    matrix_mult = MatrixMultiplication(A, B)
    result_mapreduce = matrix_mult.multiply()

    # Умножение через NumPy для проверки
    result_numpy = A @ B

    print("\nРезультат умножения через MapReduce:")
    print(result_mapreduce)
    print("\nРезультат умножения через NumPy (для проверки):")
    print(result_numpy)
    print("\nСовпадают ли результаты?", np.allclose(result_mapreduce, result_numpy))


def demonstrate_linear_regression():
    print("\n" + "=" * 60)
    print("ДЕМОНСТРАЦИЯ: ЛИНЕЙНАЯ РЕГРЕССИЯ")
    print("=" * 60)

    # Генерируем тестовые данные
    np.random.seed(42)
    n_samples = 100
    n_features = 3

    # Истинные коэффициенты
    true_w = np.array([2.5, -1.8, 0.7])
    true_b = 3.2

    # Генерируем признаки
    X = np.random.randn(n_samples, n_features)

    # Генерируем целевую переменную с шумом
    y = X @ true_w + true_b + np.random.randn(n_samples) * 0.5

    print(f"Сгенерировано {n_samples} samples с {n_features} признаками")
    print(f"Истинные коэффициенты: w = {true_w}, b = {true_b:.2f}")

    # Обучаем модель через MapReduce
    lr = LinearRegression(X, y)
    w_mapreduce, b_mapreduce = lr.fit()

    print(f"\nКоэффициенты через MapReduce:")
    print(f"w = {w_mapreduce}")
    print(f"b = {b_mapreduce:.2f}")

    # Обучаем модель через обычный метод для проверки
    X_with_intercept = np.column_stack([X, np.ones(n_samples)])
    w_b_numpy = np.linalg.pinv(X_with_intercept) @ y
    w_numpy = w_b_numpy[:-1]
    b_numpy = w_b_numpy[-1]

    print(f"\nКоэффициенты через NumPy (для проверки):")
    print(f"w = {w_numpy}")
    print(f"b = {b_numpy:.2f}")

    # Вычисляем ошибку предсказания
    y_pred_mapreduce = lr.predict(X, w_mapreduce, b_mapreduce)
    y_pred_numpy = lr.predict(X, w_numpy, b_numpy)

    mse_mapreduce = np.mean((y - y_pred_mapreduce) ** 2)
    mse_numpy = np.mean((y - y_pred_numpy) ** 2)

    print(f"\nСреднеквадратичная ошибка:")
    print(f"MapReduce: {mse_mapreduce:.4f}")
    print(f"NumPy: {mse_numpy:.4f}")


if __name__ == "__main__":
    # Демонстрация умножения матриц
    demonstrate_matrix_multiplication()

    # Демонстрация линейной регрессии
    demonstrate_linear_regression()

