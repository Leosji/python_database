import psycopg2
from psycopg2 import sql

# Параметры подключения
DB_CONFIG = {
    "dbname": "app",
    "user": "postgres",
    "password": "qwerty190luclevMU",
    "host": "localhost",
    "port": 5432
}

# Подключение к базе данных
def connect_to_db():
    return psycopg2.connect(**DB_CONFIG)

# Добавление данных
def add_data():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        # Добавление студентов
        cur.execute("""
            INSERT INTO students (name, email, birth_date, admission_year, is_active, preferences)
            VALUES
            ('Alice', 'alice@example.com', '2000-05-15', 2018, TRUE, '{"theme": "dark", "notifications": true}'),
            ('Bob', 'bob@example.com', '1999-12-20', 2017, FALSE, '{"theme": "light", "notifications": false}')
        """)

        # Добавление курсов
        cur.execute("""
            INSERT INTO courses (name, credits, tags)
            VALUES
            ('Math 101', 3.0, ARRAY['math', 'calculus']),
            ('Physics 101', 4.0, ARRAY['physics', 'mechanics'])
        """)

        # Добавление департаментов
        cur.execute("""
            INSERT INTO departments (name, head, established)
            VALUES
            ('Science', 'Dr. Smith', '2010-08-15 10:00:00+00'),
            ('Arts', 'Dr. Brown', '2005-05-20 14:30:00+00')
        """)

        # Добавление записей на курсы
        cur.execute("""
            INSERT INTO enrollments (student_id, course_id, grade, feedback)
            VALUES
            (1, 1, 85.0, 'Excellent performance'),
            (2, 2, 90.0, 'Good understanding of concepts')
        """)

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error inserting data:", e)
    finally:
        cur.close()
        conn.close()

# Чтение данных
def read_data():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        cur.execute("SELECT * FROM students")
        students = cur.fetchall()
        print("\nStudents:")
        for student in students:
            print(student)

        cur.execute("SELECT * FROM courses")
        courses = cur.fetchall()
        print("\nCourses:")
        for course in courses:
            print(course)

        cur.execute("SELECT * FROM departments")
        departments = cur.fetchall()
        print("\nDepartments:")
        for department in departments:
            print(department)

        cur.execute("SELECT * FROM enrollments")
        enrollments = cur.fetchall()
        print("\nEnrollments:")
        for enrollment in enrollments:
            print(enrollment)

    except Exception as e:
        print("Error reading data:", e)
    finally:
        cur.close()
        conn.close()

# Обновление данных
def update_data():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        # Обновление года поступления для Alice
        cur.execute("""
            UPDATE students
            SET admission_year = 2019
            WHERE name = 'Alice'
        """)

        # Увеличение количества кредитов для Math 101
        cur.execute("""
            UPDATE courses
            SET credits = credits + 1
            WHERE name = 'Math 101'
        """)

        conn.commit()
        print("Data updated successfully!")
    except Exception as e:
        print("Error updating data:", e)
    finally:
        cur.close()
        conn.close()

# Удаление данных
def delete_data():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        # Удаление записи Bob
        cur.execute("""
            DELETE FROM students
            WHERE name = 'Bob'
        """)

        conn.commit()
        print("Data deleted successfully!")
    except Exception as e:
        print("Error deleting data:", e)
    finally:
        cur.close()
        conn.close()

# Работа с транзакциями
def transaction_example_with_savepoint():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        # Начало транзакции
        conn.autocommit = False
        print("Transaction started...")

        # Три операции до точки сохранения
        cur.execute("""
            INSERT INTO students (name, email, birth_date, admission_year)
            VALUES ('John', 'john@example.com', '2002-02-15', 2021)
        """)
        print("Inserted student: John.")

        cur.execute("""
            INSERT INTO courses (name, credits)
            VALUES ('History 101', 3)
        """)
        print("Inserted course: History 101.")

        cur.execute("""
            INSERT INTO enrollments (student_id, course_id, grade)
            VALUES (1, 1, 95)
        """)
        print("Inserted enrollment for student 1 to course 1.")

        # Создание точки сохранения
        cur.execute("SAVEPOINT savepoint_1;")
        print("Savepoint created.")

        # Две операции после точки сохранения
        try:
            cur.execute("""
                INSERT INTO courses (name, credits)
                VALUES ('Invalid Course', -1)
            """)
            print("Inserted invalid course.")  # Это не будет выполнено
        except Exception as e:
            print("Error during operation:", e)
            cur.execute("ROLLBACK TO SAVEPOINT savepoint_1;")
            print("Rolled back to savepoint_1.")

        cur.execute("""
            INSERT INTO courses (name, credits)
            VALUES ('Philosophy 101', 2)
        """)
        print("Inserted course: Philosophy 101.")

        # Три операции после отката к точке сохранения
        cur.execute("""
            INSERT INTO students (name, email, birth_date, admission_year)
            VALUES ('Alice', 'alice_new@example.com', '2003-03-25', 2022)
        """)
        print("Inserted student: Alice.")

        cur.execute("""
            UPDATE students
            SET admission_year = 2020
            WHERE name = 'Alice'
        """)
        print("Updated admission year for Alice.")

        cur.execute("""
            DELETE FROM enrollments WHERE student_id = 1 AND course_id = 1
        """)
        print("Deleted enrollment for student 1 from course 1.")

        # Завершение транзакции
        conn.commit()
        print("Transaction committed successfully.")

    except Exception as e:
        print("Transaction failed:", e)
        conn.rollback()  # Полный откат транзакции
    finally:
        cur.close()
        conn.close()


# Основная программа
if __name__ == "__main__":
    print("Inserting data...")
    add_data()

    print("\nReading data...")
    read_data()

    print("\nUpdating data...")
    update_data()

    print("\nReading data after update...")
    read_data()

    print("\nDeleting data...")
    delete_data()

    print("\nReading data after delete...")
    read_data()

    print("\nTransaction example...")
    transaction_example_with_savepoint()
