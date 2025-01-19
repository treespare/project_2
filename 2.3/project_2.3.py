import psycopg2
import logging

# Настройка логгирования
logging.basicConfig(
    filename="account_balance_update.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def execute_query(conn, query):
    """
    Выполняет SQL-запрос.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            logging.info("Запрос выполнен успешно.")
    except Exception as e:
        logging.error(f"Ошибка выполнения запроса: {e}")
        raise

def fix_account_in_sum(conn):
    """
    Исправляет значения account_in_sum, основываясь на account_out_sum предыдущего дня.
    """
    query = """
    WITH corrected_data AS (
        SELECT 
            ab1.account_rk,
            ab1.effective_date,
            ab2.account_out_sum AS correct_account_in_sum
        FROM rd.account_balance ab1
        LEFT JOIN rd.account_balance ab2 
            ON ab1.account_rk = ab2.account_rk
           AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
        WHERE ab1.account_in_sum <> ab2.account_out_sum
    )
    UPDATE rd.account_balance ab
    SET account_in_sum = cd.correct_account_in_sum
    FROM corrected_data cd
    WHERE ab.account_rk = cd.account_rk
      AND ab.effective_date = cd.effective_date;
    """
    execute_query(conn, query)

def fix_account_out_sum(conn):
    """
    Исправляет значения account_out_sum, основываясь на account_in_sum следующего дня.
    """
    query = """
    WITH corrected_data AS (
        SELECT 
            ab1.account_rk,
            ab1.effective_date,
            ab2.account_in_sum AS correct_account_out_sum
        FROM rd.account_balance ab1
        LEFT JOIN rd.account_balance ab2 
            ON ab1.account_rk = ab2.account_rk
           AND ab1.effective_date + INTERVAL '1 day' = ab2.effective_date
        WHERE ab1.account_out_sum <> ab2.account_in_sum
    )
    UPDATE rd.account_balance ab
    SET account_out_sum = cd.correct_account_out_sum
    FROM corrected_data cd
    WHERE ab.account_rk = cd.account_rk
      AND ab.effective_date = cd.effective_date;
    """
    execute_query(conn, query)

def refresh_vitrina(conn):
    """
    Выполняет обновление витрины dm.account_balance_turnover.
    """
    query = """
    DELETE FROM dm.account_balance_turnover;

    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT 
        ab.account_rk,
        COALESCE(dc.currency_name, '-1') AS currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM rd.account_balance ab
    JOIN rd.account a ON ab.account_rk = a.account_rk
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
       AND dc.effective_from_date <= ab.effective_date
       AND dc.effective_to_date >= ab.effective_date
    WHERE ab.account_in_sum IS NOT NULL
      AND ab.account_out_sum IS NOT NULL;
    """
    execute_query(conn, query)

def analyze_missing_data(conn):
    """
    Анализирует пропущенные строки в витрине dm.account_balance_turnover.
    """
    try:
        logging.info("Анализ пропущенных данных в витрине по датам эффективности.")
        with conn.cursor() as cursor:
            query = """
            WITH source_data AS (
                SELECT 
                    ab.account_rk,
                    ab.effective_date,
                    ab.account_in_sum,
                    ab.account_out_sum
                FROM rd.account_balance ab
                JOIN rd.account a ON ab.account_rk = a.account_rk
            ),
            vitrina_data AS (
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum,
                    account_out_sum
                FROM dm.account_balance_turnover
            ),
            missing_data AS (
                SELECT 
                    sd.account_rk,
                    sd.effective_date
                FROM source_data sd
                LEFT JOIN vitrina_data vd 
                    ON sd.account_rk = vd.account_rk
                   AND sd.effective_date = vd.effective_date
                WHERE vd.account_rk IS NULL
            )
            SELECT COUNT(*) AS missing_count
            FROM missing_data;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            missing_count = result[0]
            logging.info(f"Количество пропущенных строк в витрине: {missing_count}.")
            return missing_count
    except Exception as e:
        logging.error(f"Ошибка при анализе пропущенных данных: {e}")
        raise

def main():
    """
    Основной процесс: исправление данных, анализ, обновление витрины.
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

        # Исправление значений account_in_sum и account_out_sum
        fix_account_in_sum(conn)
        fix_account_out_sum(conn)

        # Анализ пропущенных данных
        missing_count = analyze_missing_data(conn)
        if missing_count > 0:
            logging.info("Обнаружены пропущенные данные. Рекомендуется обновление витрины.")
        else:
            logging.info("Пропущенных данных не обнаружено. Витрина актуальна.")

        # Обновление витрины
        refresh_vitrina(conn)

    except Exception as e:
        logging.error(f"Ошибка выполнения процесса: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Подключение к базе данных закрыто.")

if __name__ == "__main__":
    main()
