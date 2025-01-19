import psycopg2
import pandas as pd

# Укажите параметры подключения к базе данных PostgreSQL
db_params = {
    "host": "localhost",
    "database": "dwh",
    "user": "postgres",
    "password": "password"
}

# Укажите путь к файлу CSV
file_path = "E:/Treespare/Рабочий стол/Новая папка (6)/Проект 2/файлы/deal_info.csv"

try:
    # Чтение данных из файла CSV
    df = pd.read_csv(file_path, encoding="Windows-1251")

    # Установление соединения с базой данных
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Удаляем существующие данные из таблицы (если необходимо)
    cursor.execute("TRUNCATE TABLE rd.deal RESTART IDENTITY;")
    conn.commit()

    # Вставка данных в PostgreSQL
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO rd.deal (deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, agreement_rk, 
                                 deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['deal_rk'], row['deal_num'], row['deal_name'], row['deal_sum'], row['client_rk'],
            row['account_rk'], row['agreement_rk'], row['deal_start_date'], row['department_rk'],
            row['product_rk'], row['deal_type_cd'], row['effective_from_date'], row['effective_to_date']
        ))
    conn.commit()

    print("Данные успешно загружены в таблицу rd.deal.")

except Exception as e:
    print(f"Ошибка: {e}")

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()
