import logging
import os
import pandas as pd
import psycopg2

# Настройка логгирования
logging.basicConfig(
    filename="loan_holiday_update.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_csv_to_table(conn, csv_path, table_name, required_columns):
    """
    Загрузка данных из CSV в PostgreSQL.
    """
    def read_csv_with_encoding(file_path):
        """Читает CSV-файл с автоматическим подбором кодировки."""
        try:
            return pd.read_csv(file_path, encoding="utf-8"), "utf-8"
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding="Windows-1251"), "Windows-1251"

    try:
        logging.info(f"Загрузка данных из {csv_path} в таблицу {table_name}.")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл не найден: {csv_path}")

        df, encoding_used = read_csv_with_encoding(csv_path)
        logging.info(f"Файл {csv_path} успешно прочитан с кодировкой {encoding_used}.")

        # Учитываем порядок столбцов
        df = df[required_columns]

        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                cursor.execute(f"""
                    INSERT INTO {table_name} ({', '.join(required_columns)})
                    VALUES ({', '.join(['%s'] * len(required_columns))})
                    ON CONFLICT DO NOTHING;
                """, tuple(row))
        conn.commit()
        logging.info(f"Данные из {csv_path} успешно загружены в таблицу {table_name}.")
    except Exception as e:
        logging.error(f"Ошибка загрузки данных из {csv_path} в таблицу {table_name}: {e}")
        raise


def analyze_missing_data(conn):
    """
    Анализирует пропущенные строки в витрине dm.loan_holiday_info.
    """
    try:
        logging.info("Анализ пропущенных данных в витрине.")
        with conn.cursor() as cursor:
            query = """
            WITH source_data AS (
                SELECT 
                    d.deal_rk, d.effective_from_date, d.effective_to_date, 
                    p.product_name, lh.loan_holiday_type_cd
                FROM rd.deal d
                LEFT JOIN rd.loan_holiday lh ON d.deal_rk = lh.deal_rk
                LEFT JOIN rd.product p ON d.product_rk = p.product_rk
            ),
            missing_data AS (
                SELECT sd.*
                FROM source_data sd
                LEFT JOIN dm.loan_holiday_info lhi 
                    ON sd.deal_rk = lhi.deal_rk
                   AND sd.effective_from_date = lhi.effective_from_date
                WHERE lhi.deal_rk IS NULL
            )
            SELECT COUNT(*) AS missing_count FROM missing_data;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            missing_count = result[0]
            logging.info(f"Найдено {missing_count} пропущенных строк.")
            return missing_count
    except Exception as e:
        logging.error(f"Ошибка при анализе пропущенных данных: {e}")
        raise


def refresh_vitrina(conn):
    """
    Выполняет обновление витрины dm.loan_holiday_info.
    """
    try:
        logging.info("Обновление витрины dm.loan_holiday_info.")
        with conn.cursor() as cursor:
            query = """
            DELETE FROM dm.loan_holiday_info;

            INSERT INTO dm.loan_holiday_info (
                deal_rk, effective_from_date, effective_to_date, agreement_rk, client_rk,
                department_rk, product_rk, product_name, deal_type_cd, deal_start_date, 
                deal_name, deal_number, deal_sum, loan_holiday_type_cd, loan_holiday_start_date,
                loan_holiday_finish_date, loan_holiday_fact_finish_date, 
                loan_holiday_finish_flg, loan_holiday_last_possible_date
            )
            SELECT 
                d.deal_rk, d.effective_from_date, d.effective_to_date, 
                d.agreement_rk, d.client_rk, d.department_rk, d.product_rk, 
                p.product_name, d.deal_type_cd, d.deal_start_date, d.deal_name,
                d.deal_num, d.deal_sum, lh.loan_holiday_type_cd, lh.loan_holiday_start_date,
                lh.loan_holiday_finish_date, lh.loan_holiday_fact_finish_date, 
                lh.loan_holiday_finish_flg, lh.loan_holiday_last_possible_date
            FROM rd.deal d
            LEFT JOIN rd.loan_holiday lh ON d.deal_rk = lh.deal_rk
            LEFT JOIN rd.product p ON d.product_rk = p.product_rk;
            """
            cursor.execute(query)
            conn.commit()
            logging.info("Витрина dm.loan_holiday_info успешно обновлена.")
    except Exception as e:
        logging.error(f"Ошибка при обновлении витрины: {e}")
        raise


def determine_loading_strategy(conn):
    """
    Определяет, какой способ загрузки данных выбрать: дозагрузка или полная перезагрузка.
    Возвращает строку: 'partial' для дозагрузки или 'full' для полной перезагрузки.
    """
    try:
        with conn.cursor() as cursor:
            # Подсчёт общего количества строк в витрине
            cursor.execute("SELECT COUNT(*) FROM dm.loan_holiday_info;")
            total_rows = cursor.fetchone()[0]

            # Подсчёт количества пропущенных строк
            cursor.execute("""
            WITH source_data AS (
                SELECT 
                    d.deal_rk,
                    d.effective_from_date,
                    d.effective_to_date
                FROM rd.deal d
                LEFT JOIN rd.loan_holiday lh ON d.deal_rk = lh.deal_rk
                LEFT JOIN rd.product p ON d.product_rk = p.product_rk
            ),
            missing_data AS (
                SELECT sd.*
                FROM source_data sd
                LEFT JOIN dm.loan_holiday_info lhi 
                    ON sd.deal_rk = lhi.deal_rk
                   AND sd.effective_from_date = lhi.effective_from_date
                WHERE lhi.deal_rk IS NULL
            )
            SELECT COUNT(*) FROM missing_data;
            """)
            missing_count = cursor.fetchone()[0]

            logging.info(f"Общее количество строк в витрине: {total_rows}")
            logging.info(f"Количество пропущенных строк: {missing_count}")

            # Определение стратегии
            if missing_count == 0:
                logging.info("Все строки присутствуют. Загрузка не требуется.")
                return None
            elif missing_count / (total_rows + 1) > 0.3:  # Пропущено более 30% строк
                logging.info("Пропущено более 30% данных. Рекомендуется полная перезагрузка.")
                return "full"
            else:
                logging.info("Пропущено менее 30% данных. Рекомендуется частичная загрузка.")
                return "partial"
    except Exception as e:
        logging.error(f"Ошибка при определении стратегии загрузки: {e}")
        raise


def main():
    """
    Основной процесс: загрузка данных, анализ, выбор стратегии загрузки, обновление витрины.
    """
    conn = None
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host="localhost",
            database="dwh",
            user="postgres",
            password="password"
        )
        logging.info("Подключение к базе данных успешно установлено.")

        # Загрузка данных из CSV
        file_directory = r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 2\файлы"

        load_csv_to_table(conn, os.path.join(file_directory, "deal_info.csv"), "rd.deal", [
            "deal_rk", "deal_num", "deal_name", "deal_sum", "client_rk",
            "agreement_rk", "deal_start_date", "department_rk",
            "product_rk", "deal_type_cd", "effective_from_date",
            "effective_to_date", "account_rk"
        ])
        load_csv_to_table(conn, os.path.join(file_directory, "product_info.csv"), "rd.product", [
            "product_rk", "product_name", "effective_from_date", "effective_to_date"
        ])

        # Определение стратегии загрузки
        strategy = determine_loading_strategy(conn)

        # Выполнение загрузки в зависимости от выбранной стратегии
        if strategy == "full":
            logging.info("Выбрана полная перезагрузка витрины.")
            refresh_vitrina(conn)
        elif strategy == "partial":
            logging.info("Выбрана частичная загрузка витрины.")
            missing_count = analyze_missing_data(conn)
            if missing_count > 0:
                refresh_vitrina(conn)  # Можно добавить только частичную дозагрузку
        else:
            logging.info("Загрузка не требуется. Все данные присутствуют.")

    except Exception as e:
        logging.error(f"Ошибка выполнения процесса: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Подключение к базе данных закрыто.")


if __name__ == "__main__":
    main()

#SELECT * FROM dm.loan_holiday_info;