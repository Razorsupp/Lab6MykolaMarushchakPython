import mysql.connector
import pandas as pd
from mysql.connector import Error

def Connect_to_DB(server, login, passw, DBname=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=server,
            user=login,
            password=passw,
            database=DBname
        )
        print("Встановлено звязок з БД!")
    except Error as e:
        print(f"Помилка з'єднанні з БД: {e}")
    return connection

def CreateDataBase(connection):
    try:
        cursor = connection.cursor()
        database_name = "Company"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База даних '{database_name}' успішно створена")
    except Error as e:
        print(f"Помилка при створенні БД: {e}")

def CreateTables(connection):
    try:
        database_name = "Company"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Errors (
                ErrorID INT AUTO_INCREMENT PRIMARY KEY,
                ErrorCode VARCHAR(10),
                Description VARCHAR(255),
                DateReceived DATE,
                ErrorLevel ENUM('критична', 'важлива', 'незначна'),
                FunctionalityCategory ENUM('інтерфейс', 'дані', 'розрахунковий алгоритм', 'інше', 'невідома категорія'),
                Source ENUM('користувач', 'тестувальник')
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Programmers (
                ProgrammerID INT AUTO_INCREMENT PRIMARY KEY,
                Surname VARCHAR(30),
                Name VARCHAR(30),
                Phone VARCHAR(13)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS BugFixes (
                FixID INT AUTO_INCREMENT PRIMARY KEY,
                ErrorCode INT,
                StartDate DATE,
                FixDurationDay INT CHECK (FixDurationDay > 0),
                ProgrammerCode INT,
                WorkCostPerDay DECIMAL(10, 2),
                FOREIGN KEY (ErrorCode) REFERENCES Errors(ErrorID),
                FOREIGN KEY (ProgrammerCode) REFERENCES Programmers(ProgrammerID)
            )
        """)


        connection.commit()
        print("Створено таблиці!")
    except Error as e:
        print(f"Помилка при створенні таблиць: {e}")


def InsertInfoTables(connection):
    query = (f"""
        INSERT INTO Errors (ErrorID, ErrorCode, Description, DateReceived, ErrorLevel, FunctionalityCategory, Source)
        VALUES
        (1, 'P001', 'Невідома помилка', '2023-11-10', 'Критична', 'Інше', 'Користувач'),
        (2, 'P002', 'Помилка завантаження', '2023-11-09', 'Важлива', 'Дані', 'Тестувальник'),
        (3, 'P003', 'Помилка збереження', '2023-11-08', 'Незначна', 'Інше', 'Тестувальник'),
        (4, 'P004', 'Помилка виводу на екран', '2023-11-07', 'Критична', 'Інтерфейс', 'Користувач'),
        (5, 'P005', 'Помилка авторизації', '2023-11-06', 'Важлива', 'Дані', 'Тестувальник'),
        (6, 'P006', 'Помилка видалення', '2023-11-05', 'Незначна', 'Інше', 'Тестувальник'),
        (7, 'P007', 'Помилка пошуку', '2023-11-04', 'Критична', 'Розрахунковий алгоритм', 'Користувач'),
        (8, 'P008', 'Помилка внесення даних', '2023-11-03', 'Важлива', 'Дані', 'Тестувальник'),
        (9, 'P009', 'Помилка відображення', '2023-11-02', 'Незначна', 'Інтерфейс', 'Тестувальник'),
        (10, 'P010', 'Помилка синхронізації', '2023-11-01', 'Критична', 'Інше', 'Користувач'),
        (11, 'P011', 'Помилка підрахунку', '2023-10-31', 'Важлива', 'Розрахунковий алгоритм', 'Тестувальник'),
        (12, 'P012', 'Помилка звіту', '2023-10-30', 'Незначна', 'Інтерфейс', 'Тестувальник'),
        (13, 'P013', 'Помилка імпорту', '2023-10-29', 'Критична', 'Дані', 'Користувач'),
        (14, 'P014', 'Помилка експорту', '2023-10-28', 'Важлива', 'Дані', 'Тестувальник'),
        (15, 'P015', 'Помилка валідації', '2023-10-27', 'Незначна', 'Розрахунковий алгоритм', 'Тестувальник'),
        (16, 'P016', 'Помилка з`єднання', '2023-10-26', 'Критична', 'Інтерфейс', 'Користувач'),
        (17, 'P017', 'Помилка сортування', '2023-10-25', 'Важлива', 'Розрахунковий алгоритм', 'Тестувальник'),
        (18, 'P018', 'Помилка відновлення', '2023-10-24', 'Незначна', 'Інше', 'Тестувальник'),
        (19, 'P019', 'Помилка реєстрації', '2023-10-23', 'Критична', 'Дані', 'Користувач'),
        (20, 'P020', 'Помилка розгортання', '2023-10-22', 'Важлива', 'Інше', 'Тестувальник');
    """)
    Query(connection, query)

    query = ("""
        INSERT INTO Programmers (ProgrammerID, Surname, Name, Phone)
        VALUES
        (1, 'Іванов', 'Іван', '+380631234567'),
        (2, 'Петров', 'Петро', '+380979876543'),
        (3, 'Сидоров', 'Олександр', '+380501112233'),
        (4, 'Коваленко', 'Ольга', '+380684455667');
    """)
    Query(connection, query)

    query = ('''
        INSERT INTO BugFixes (FixID, ErrorCode, StartDate, FixDurationDay, ProgrammerCode, WorkCostPerDay)
        VALUES
        (1, 1, '2023-11-10', 2, 1, 1500.00),
        (2, 2, '2023-11-11', 3, 3, 1800.00),
        (3, 3, '2023-11-12', 1, 2, 1200.00),
        (4, 4, '2023-11-13', 2, 4, 1600.00),
        (5, 5, '2023-11-14', 2, 1, 1500.00),
        (6, 6, '2023-11-15', 3, 3, 1800.00),
        (7, 7, '2023-11-16', 1, 2, 1200.00),
        (8, 8, '2023-11-17', 2, 4, 1600.00),
        (9, 9, '2023-11-18', 2, 1, 1500.00),
        (10, 10, '2023-11-19', 3, 3, 1800.00),
        (11, 11, '2023-11-20', 1, 2, 1200.00),
        (12, 12, '2023-11-21', 2, 4, 1600.00),
        (13, 13, '2023-11-22', 2, 1, 1500.00),
        (14, 14, '2023-11-23', 3, 3, 1800.00);
    ''')
    Query(connection, query)

def Query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запит виконано!")
    except Error as e:
        print(f"Помилка при виконанні запиту: {e}")

def printQuery(connection, query, params=None):
    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        print(df)
        print("Запит виконано!")
    except Error as e:
        print(f"Помилка при виконанні запиту: {e}")
    finally:
        cursor.close()

def TaskQuery(connection):
    connect = connection
    print("Відобразити всі критичні помилки. Відсортувати по коду помилки")
    query = ("""
        SELECT * FROM Errors WHERE ErrorLevel = 'критична' ORDER BY ErrorCode;
    """)
    printQuery(connect, query)

    print("Порахувати кількість помилок кожного рівня (підсумковий запит)")
    query = ("""
        SELECT ErrorLevel, COUNT(*) AS ErrorCount FROM Errors GROUP BY ErrorLevel;
    """)
    printQuery(connect, query)

    print("Порахувати вартість роботи програміста при виправленні кожної помилки (запит з обчислювальним полем)")
    query = ("""
        SELECT BugFixes.FixID, BugFixes.ErrorCode, BugFixes.ProgrammerCode, (BugFixes.WorkCostPerDay * BugFixes.FixDurationDay) AS TotalWorkCost
        FROM BugFixes;
    """)
    printQuery(connect, query)

    print("Відобразити всі помилки, які надійшли із заданого джерела (запит з параметром)")
    Source = input()
    query = ("""
        SELECT * FROM Errors WHERE Source = %s;
    """)
    printQuery(connect, query, (Source,))

    print("Порахувати кількість помилок, які надійшли від користувачів та тестувальників (підсумковий запит)")
    query = ("""
        SELECT Source, COUNT(*) AS ErrorCount FROM Errors WHERE Source IN ('користувач', 'тестувальник') GROUP BY Source;
    """)
    printQuery(connect, query)

    print("Порахувати кількість критичних, важливих, незначних помилок, виправлених кожним програмістом (перехресний запит)")
    query = ("""
        SELECT BugFixes.ProgrammerCode, 
            COUNT(CASE WHEN Errors.ErrorLevel = 'критична' THEN 1 END) AS CriticalErrors,
            COUNT(CASE WHEN Errors.ErrorLevel = 'важлива' THEN 1 END) AS ImportantErrors,
            COUNT(CASE WHEN Errors.ErrorLevel = 'незначна' THEN 1 END) AS MinorErrors
        FROM BugFixes
        JOIN Errors ON BugFixes.ErrorCode = Errors.ErrorID
        GROUP BY BugFixes.ProgrammerCode;
    """)
    printQuery(connect, query)

if __name__ == "__main__":
    config = {
        'server': '127.0.0.1',
        'login': 'root',
        'passw': 'root',
        'DBname': 'Company',
    }

    conn = Connect_to_DB(**config)

    CreateDataBase(conn)
    CreateTables(conn)
    InsertInfoTables(conn)

    conn = Connect_to_DB(**config)
    TaskQuery(conn)
    conn.close()

    print("БД успішно створена, таблиці заповнено!")