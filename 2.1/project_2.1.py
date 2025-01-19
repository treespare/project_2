import logging
import psycopg2
from psycopg2 import sql

# Настройка логгирования
logging.basicConfig(
    filename="remove_duplicates.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def remove_duplicates():
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host="localhost",
            database="dwh",
            user="postgres",
            password="password"
        )
        cursor = conn.cursor()
        logging.info("Подключение к базе данных успешно установлено.")

        # Поиск дублей
        logging.info("Начинается поиск дублей в таблице dm.client.")
        find_duplicates_query = """
            SELECT 
                client_rk, 
                effective_from_date, 
                COUNT(*) AS duplicate_count
            FROM dm.client
            GROUP BY client_rk, effective_from_date
            HAVING COUNT(*) > 1;
        """
        cursor.execute(find_duplicates_query)
        duplicates = cursor.fetchall()

        if not duplicates:
            logging.info("Дубли в таблице не найдены.")
            return

        logging.info(f"Найдено {len(duplicates)} дублирующих групп записей.")

        # Удаление дублей
        logging.info("Начинается удаление дублей.")
        delete_duplicates_query = """
            WITH CTE AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY client_rk, effective_from_date
                        ORDER BY effective_to_date DESC
                    ) AS row_num
                FROM dm.client
            )
            DELETE FROM dm.client
            WHERE client_rk IN (
                SELECT client_rk 
                FROM CTE 
                WHERE row_num > 1
            );
        """
        cursor.execute(delete_duplicates_query)
        conn.commit()
        logging.info("Дубли успешно удалены.")

    except psycopg2.Error as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("Подключение к базе данных закрыто.")


if __name__ == "__main__":
    remove_duplicates()

