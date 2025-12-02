import telegram
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
)
from telegram.error import TimedOut
from httpx import ConnectError
import pandas as pd
import datetime
import json
import os
import re # Для перевірки формату ID
import logging # Для журналу
import configparser
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from apscheduler.schedulers.asyncio import AsyncIOScheduler # Для асинхронних ботів
from apscheduler.triggers.cron import CronTrigger
import asyncio # Якщо ви ще не імпортували для асинхронності
import signal # Імпортуємо модуль signal
from pytz import timezone # pip install pytz
import numpy as np
from scipy import stats

DEBUG = False
is_bot_in_group = True

# --- НАЛАШТУВАННЯ ЖУРНАЛУ ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
# Налаштування логування для APScheduler (для дебагу)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)
# Налаштування логування для asyncio (для дебагу)
logging.getLogger('asyncio').setLevel(logging.DEBUG)

# --- ЗАВАНТАЖЕННЯ КОНСТАНТ З CONFIG.INI ---
config = configparser.ConfigParser()

# Глобальні змінні з конфігурації
TOKEN = ""
ADMIN_IDS = []
GROUP_ID = ""
STATUS_FILE = ""
BANLIST = []
SERVICE_ACCOUNT_KEY_PATH = ""
SPREADSHEET_ID = ""
SHEET_NAME = ""
STATS_SHEET_ID = ""
STATS_WORKSHEET_NAME = ""

# Глобальні змінні для Google Sheets
SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEETS_SERVICE = None
CREDS = None

# Глобальний DataFrame
queue_df = None

# Функція для збереження config.ini
def save_config():
    '''
    [BOT_SETTINGS]
    TOKEN = ВАШ_ТОКЕН_БОТА ; Можна отримати через @BotFather /token (лишається постійним)
    ADMIN_IDS = 1193718147,512749402 ; Можна отримати через @userinfobot

    [GOOGLE_SHEETS]
    SERVICE_ACCOUNT_KEY_PATH = service_account_key.json ; Шлях до вашого JSON-ключа
    SPREADSHEET_ID = ІДЕНТИФІКАТОР_ВАШОЇ_ТАБЛИЦІ ; Довгий рядок у URL таблиці (наприклад, 1Bxx...)
    SHEET_NAME = TODO ; Назва листа в таблиці (зазвичай "Аркуш1" або "Sheet1")
    '''
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

def initialize_bot():
    global TOKEN, ADMIN_IDS, GROUP_ID, STATUS_FILE, BANLIST
    global SERVICE_ACCOUNT_KEY_PATH, SPREADSHEET_ID, SHEET_NAME
    global STATS_SHEET_ID, STATS_WORKSHEET_NAME
    global SHEETS_SERVICE, CREDS, queue_df

    try:
        config.read('config.ini') # Назва файлу конфігурації
        
        # Отримуємо значення з секції BOT_SETTINGS
        TOKEN = config['BOT_SETTINGS']['TOKEN']
        # ADMIN_IDS: розбиваємо рядок на список чисел
        admin_ids_str = config['BOT_SETTINGS']['ADMIN_IDS']
        # Оновлюємо ADMIN_IDS як список цілих чисел
        ADMIN_IDS = [int(id_str.strip()) for id_str in admin_ids_str.split(',') if id_str.strip()]
        GROUP_ID = config['BOT_SETTINGS']['GROUP_ID']
        # Отримуємо значення з секції STATUS
        STATUS_FILE = config['BOT_SETTINGS']['STATUS_FILE']
        # BANLIST: розбиваємо рядок на список чисел
        ban_ids_str = config['BOT_SETTINGS']['BANLIST']
        # Оновлюємо ADMIN_IDS як список цілих чисел
        BANLIST = [int(id_str.strip()) for id_str in ban_ids_str.split(',') if id_str.strip()]    

        # Отримуємо значення з секції GOOGLE_SHEETS
        SERVICE_ACCOUNT_KEY_PATH = config['GOOGLE_SHEETS']['SERVICE_ACCOUNT_KEY_PATH']
        SPREADSHEET_ID = config['GOOGLE_SHEETS']['SPREADSHEET_ID']
        SHEET_NAME = config['GOOGLE_SHEETS']['SHEET_NAME']
        STATS_SHEET_ID = config['GOOGLE_SHEETS']['STATS_SHEET_ID']
        STATS_WORKSHEET_NAME = config['GOOGLE_SHEETS']['STATS_WORKSHEET_NAME']
        
        logger.info("Константи успішно завантажено з config.ini")

    except KeyError as e:
        logger.error(f"Помилка: Не знайдено ключ '{e}' у файлі config.ini. Перевірте, чи всі налаштування присутні.")
        if __name__ == "__main__":
            exit(1)
    except FileNotFoundError:
        logger.error("Помилка: Файл config.ini не знайдено. Будь ласка, створіть його.")
        if __name__ == "__main__":
            exit(1)
    except Exception as e:
        logger.error(f"Невідома помилка при читанні config.ini: {e}")
        if __name__ == "__main__":
            exit(1)

    # --- Налаштування Google Sheets API ---
    try:
        CREDS = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH, scopes=SERVICE_ACCOUNT_SCOPES
        )
        SHEETS_SERVICE = build('sheets', 'v4', credentials=CREDS)
        logger.info("Успішно підключено до Google Sheets API.")
    except FileNotFoundError:
        logger.error(f"Помилка: Файл ключа сервісного облікового запису не знайдено за шляхом: {SERVICE_ACCOUNT_KEY_PATH}")
        if __name__ == "__main__":
            exit(1)
    except Exception as e:
        logger.error(f"Помилка ініціалізації Google Sheets API: {e}")
        if __name__ == "__main__":
            exit(1)
            
    # Ініціалізація DataFrame при запуску
    queue_df = load_queue_data()

# Стан для ConversationHandler для запису (/join)
JOIN_GETTING_ID, JOIN_GETTING_DATE = range(2)
# Стан для ConversationHandler для скасування (/cancel_record)
CANCEL_GETTING_ID = range(2, 3) # Починаємо з 2, щоб уникнути конфліктів з попередніми станами
# Стан для ConversationHandler для відображення (/show)
SHOW_GETTING_OPTION, SHOW_GETTING_DATE = range(3, 5) # Починаємо з 3, щоб уникнути конфліктів з попередніми
# Стан для ConversationHandler для перегляду статусу (/status)
STATUS_GETTING_ID = range(5, 6) # Новий стан для запиту ID


# --- НАЗВИ СТОВПЦІВ ---
REQUIRED_COLUMNS = ['ID', 'Дата', 'Примітки', 'Статус', 'Змінено', 'Попередня дата', 'TG ID', 'TG Name', 'TG Full Name']
days_ahead = 15 # Кількість кнопок днів, які ми хочемо показати


# --- ДОПОМІЖНІ ФУНКЦІЇ ДЛЯ ПРОГНОЗУВАННЯ ---
def get_ordinal_date(date_obj):
    # Якірна дата: 5 січня 1970 року (понеділок)
    anchor = datetime.date(1970, 1, 5)
    diff = (date_obj - anchor).days
    weeks = diff // 7
    days = diff % 7
    return weeks * 5 + min(days, 5)

def get_date_from_ordinal(ordinal):
    anchor = datetime.date(1970, 1, 5)
    weeks = int(ordinal) // 5
    days = int(ordinal) % 5
    total_days = weeks * 7 + days
    return anchor + datetime.timedelta(days=total_days)

def calculate_prediction(user_id, stats_df):
    if stats_df is None or stats_df.empty:
        return None
        
    df = stats_df.copy()
    # Перетворення в числовий формат та формат дати
    df['id'] = pd.to_numeric(df['Останній номер що зайшов'], errors='coerce')
    df['date'] = pd.to_datetime(df['Дата прийому'], format='%d.%m.%Y', dayfirst=True, errors='coerce').dt.date
    df = df.dropna(subset=['id', 'date'])
    
    if len(df) < 5: # Потрібна достатня кількість точок
        return None

    df['ordinal'] = df['date'].apply(get_ordinal_date)
    df = df.sort_values('ordinal')
    
    X = df['id'].values
    Y = df['ordinal'].values
    n = len(X)
    
    # Експоненційні ваги
    weights = np.exp(-3 + (np.arange(n) / (n - 1)) * 3)
    
    sumW = np.sum(weights)
    sumWX = np.sum(weights * X)
    sumWY = np.sum(weights * Y)
    # зважена лінійна регресія
    sumWXX = np.sum(weights * X**2)
    sumWXY = np.sum(weights * X * Y)
    
    denom = sumW * sumWXX - sumWX**2
    if denom == 0:
        return None
        
    slope = (sumW * sumWXY - sumWX * sumWY) / denom
    intercept = (sumWY - slope * sumWX) / sumW
    
    # Статистика для інтервалів
    weightedMeanX = sumWX / sumW
    # Зважена дисперсія X
    weightedVarX = np.sum(weights * (X - weightedMeanX)**2)
    
    yPred = slope * X + intercept
    residuals = Y - yPred
    weightedSumResSq = np.sum(weights * residuals**2)
    
    dof = sumW - 2
    if dof <= 0:
        return None
        
    mseWeighted = weightedSumResSq / dof
    
    # T-показники
    tScore90 = stats.t.ppf(0.95, dof)
    tScore50 = stats.t.ppf(0.75, dof)
    
    predOrd = slope * user_id + intercept
    
    term3 = (user_id - weightedMeanX)**2 / weightedVarX
    sePred = np.sqrt(mseWeighted * (1 + 1/sumW + term3))
    
    margin90 = tScore90 * sePred
    margin50 = tScore50 * sePred
    
    # Межі
    l90_ord = predOrd - margin90
    h90_ord = predOrd + margin90
    l50_ord = predOrd - margin50
    h50_ord = predOrd + margin50
    
    # Обмеження майбутнім (наступний робочий день після останньої історичної дати)
    max_hist_ord = df['ordinal'].max()
    min_feasible = max_hist_ord + 1
    
    # Обмежуємо нижню межу, тільки якщо ID в майбутньому (більше максимального історичного ID)
    if user_id > df['id'].max():
        l90_ord = max(l90_ord, min_feasible)
        l50_ord = max(l50_ord, min_feasible)
        # Перевірка валідності початку діапазону
    
    return {
        'l90': get_date_from_ordinal(l90_ord),
        'l50': get_date_from_ordinal(l50_ord),
        'mean': get_date_from_ordinal(predOrd),
        'h50': get_date_from_ordinal(h50_ord),
        'h90': get_date_from_ordinal(h90_ord),
        'dist': {
            'loc': predOrd,
            'scale': sePred,
            'df': dof
        }
    }

# Завантаження даних з Google Sheet або створення нового DataFrame
def load_queue_data() -> pd.DataFrame | None:
    """Завантажує дані черги з Google Sheet."""
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо завантажити дані.")
        return None # Повертаємо None при неініціалізованому сервісі

    try:
        # Отримуємо всі записи з аркуша, починаючи з A1
        range_name = f"{SHEET_NAME}!A:{chr(ord('A') + len(REQUIRED_COLUMNS) - 1)}" # Задаємо діапазон для читання
        result = SHEETS_SERVICE.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()     
        values = result.get('values', [])

        if not values: # Якщо аркуш порожній (для діапазону A:H)
            logger.warning("Google Sheet порожній. Ініціалізація заголовків.")
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        
        # Заголовки - це перший рядок, починаючи з другого елемента (індекс 1)
        columns = values[0]
        # Дані з другого рядка
        data = values[1:]

        expected_num_columns = len(REQUIRED_COLUMNS)
        processed_data = []
        for row in data:
            if len(row) < expected_num_columns:
                # Доповнюємо рядок порожніми рядками, якщо він коротший
                processed_row = row + [''] * (expected_num_columns - len(row))
            elif len(row) > expected_num_columns:
                # Обрізаємо рядок, якщо він довший, ніж очікувана кількість колонок
                processed_row = row[:expected_num_columns]
            else:
                processed_row = row
            processed_data.append(processed_row)

        # !!! Важливо: Використовуємо REQUIRED_COLUMNS як заголовки для створення DataFrame.
        # Це гарантує, що DataFrame завжди матиме очікувану структуру.
        df = pd.DataFrame(processed_data, columns=REQUIRED_COLUMNS)

        logger.info(f"Дані успішно завантажено з Google Sheet. Завантажено {len(df)} записів.")
        return df

    except HttpError as err:
        logger.error(f"Google API HttpError при завантаженні даних: {err.resp.status} - {err.content}")
        return None
    except Exception as e:
        logger.error(f"Помилка завантаження даних з Google Sheet: {e}")
        return None

# Допоміжні функції для роботи зі станом і підтвердженнями
def load_status_state() -> dict:
    """Завантажує останній відомий стан статусів з JSON-файлу."""
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r", encoding='utf8') as f:
            return json.load(f)
    return {}

def save_status_state(state: dict):
    """Зберігає поточний стан статусів у JSON-файл."""
    with open(STATUS_FILE, "w", encoding='utf8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)
        
# Допоміжна функція перетворення ID в число
def extract_main_id(id_string):
    """Витягує основний номер ID з рядка."""
    if isinstance(id_string, str):
        match = re.match(r'^\d+', id_string)
        if match:
            return int(match.group())
    return None

async def get_stats_data() -> pd.DataFrame | None:
    """
    Завантажує дані з аркуша 'Stats' за допомогою Google Sheets API,
    обробляє їх і повертає DataFrame.
    """
    try:
        # 1. Виконуємо запит до API для отримання даних
        # Діапазон "A1:Z" гарантує, що ми отримаємо всі дані з аркуша
        range_name = f"{STATS_WORKSHEET_NAME}!A1:Z"
        result = SHEETS_SERVICE.spreadsheets().values().get(
            spreadsheetId=STATS_SHEET_ID, range=range_name
        ).execute()
        
        list_of_lists = result.get("values", [])

        if not list_of_lists:
            logger.warning("Аркуш 'Stats' порожній.")
            return pd.DataFrame()
        # 2. Перетворюємо список списків у DataFrame
        stats_df = pd.DataFrame(list_of_lists[1:], columns=list_of_lists[0])
        # 3. Підготовка даних (перетворення типів)
        stats_df['Останній номер що зайшов'] = stats_df['Останній номер що зайшов'].apply(extract_main_id)
        stats_df['Дата прийому'] = pd.to_datetime(stats_df['Дата прийому'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
        
        logger.info("Дані з аркуша 'Stats' успішно завантажено та оброблено.")
        return stats_df

    except HttpError as err:
        logger.error(f"Google API HttpError при завантаженні даних: {err.resp.status} - {err.content}. Перевірте адресу таблиці та права доступу.")
        return None
    except Exception as e:
        logger.error(f"Загальна помилка при завантаженні даних з 'Stats': {e}")
        return None

# Збереження даних у Google Sheet (додавання рядків)
def save_queue_data(df_to_save) -> bool:
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо зберегти дані.")
        return False
    if df_to_save.empty:
        logger.warning("Спроба зберегти порожній запис у Google Sheet. Пропущено.")
        return True # Вважаємо це успіхом, оскільки нічого не потрібно було робити

    try:
        # Підготовка даних: перетворюємо DataFrame на список списків
        # Вибираємо тільки потрібні колонки та забезпечуємо їх порядок
        data_to_append = df_to_save[REQUIRED_COLUMNS].values.tolist()

        SHEETS_SERVICE.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME,
            valueInputOption='USER_ENTERED', # Дозволяє Google розпізнавати формати
            insertDataOption='INSERT_ROWS', # Додаємо нові рядки
            body={'values': data_to_append}
        ).execute()
        
        logger.info(f"Новий запис успішно додано до Google Sheet '{SHEET_NAME}'. ID: {df_to_save.iloc[0]['ID']}")
        return True
    except HttpError as err:
        logger.error(f"Google API HttpError при збереженні даних: {err.resp.status} - {err.content}")
        return False
    except Exception as e:
        logger.error(f"Помилка збереження даних у Google Sheet: {e}")
        return False

# Функція для повного перезапису Google Sheet
def save_queue_data_full(df: pd.DataFrame) -> bool:
    """
    Повністю перезаписує Google Sheet даними з DataFrame.
    Очищає існуючі дані та записує нові, включаючи заголовки.
    """
    if SHEETS_SERVICE is None:
        logger.error("Google Sheets API не ініціалізовано. Неможливо зберегти дані.")
        return False

    try:
        # Очищаємо весь лист перед записом нових даних.
        # Зверніть увагу: це видалить ВСІ дані на листі SHEET_NAME!
        SHEETS_SERVICE.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:Z"
        ).execute()
        logger.info(f"Google Sheet '{SHEET_NAME}' було очищено перед записом.")

        if df.empty:
            logger.info(f"DataFrame для запису порожній, записано лише заголовки.")
            # Якщо DataFrame порожній, все одно записуємо заголовки
            body = {'values': [REQUIRED_COLUMNS]}
            SHEETS_SERVICE.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A1",
                valueInputOption='RAW', body=body
            ).execute()
            return

        # Забезпечуємо наявність всіх необхідних колонок у DataFrame та їх порядок
        df_to_save = df.copy()
        for col in REQUIRED_COLUMNS:
            if col not in df_to_save.columns:
                df_to_save[col] = ''
        df_to_save = df_to_save[REQUIRED_COLUMNS]

        # Конвертуємо DataFrame у список списків для запису, додаючи заголовки
        data_to_write = [df_to_save.columns.tolist()] # Заголовки
        data_to_write.extend(df_to_save.values.tolist()) # Дані

        # Записуємо дані у лист, починаючи з A1, щоб включити заголовки
        SHEETS_SERVICE.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A1",
            valueInputOption='USER_ENTERED', # Дозволяє Google розпізнавати формати
            body={'values': data_to_write}
        ).execute()
        logger.info(f"Дані успішно записано до Google Sheet '{SHEET_NAME}'.")
        return True
    except HttpError as err:
        logger.error(f"Google API HttpError при повному збереженні даних: {err.resp.status} - {err.content}")
        return False
    except Exception as e:
        logger.error(f"Помилка при повному збереженні даних у Google Sheet: {e}")
        return False



# --- СТАНДАРТНА КЛАВІАТУРА З КОМАНДАМИ ---
# Важливо: хоча на кнопках текст, для внутрішньої логіки бот все ще реагує на цей текст як на "команду"
BUTTON_TEXT_JOIN = "Записатися / Перенести"
BUTTON_TEXT_SHOW = "Переглянути чергу"
BUTTON_TEXT_CANCEL_RECORD = "Скасувати запис"
BUTTON_TEXT_OPEN_SHEET = "Відкрити таблицю"
#BUTTON_TEXT_CLEAR_QUEUE = "Очистити чергу"
BUTTON_TEXT_CANCEL_OP = "Скасувати ввід" # Для відміни поточної дії
BUTTON_TEXT_STATUS = "Переглянути статус"
# Створюємо callback_data для кнопок
CALLBACK_YES = "confirm_yes"
CALLBACK_NO = "confirm_no"

# Створюємо кнопки
# Кнопка для запису або зміни дати відвідання
button_join = KeyboardButton(BUTTON_TEXT_JOIN)
# Кнопка відображення черги
button_show = KeyboardButton(BUTTON_TEXT_SHOW)
# Кнопка для скасування запису
button_cancel_record = KeyboardButton(BUTTON_TEXT_CANCEL_RECORD)
# Кнопка для скачування таблиці
button_open_sheet = KeyboardButton(BUTTON_TEXT_OPEN_SHEET)
# Кнопка для очищення черги(відображається для всіх, але працює лише для адмінів)
#button_clear_queue = KeyboardButton(BUTTON_TEXT_CLEAR_QUEUE)
# Кнопка для скасування поточної дії
button_cancel_op = KeyboardButton(BUTTON_TEXT_CANCEL_OP)
# Кнопка для перегляду статусу останньої заявки
button_status = KeyboardButton(BUTTON_TEXT_STATUS)

# Формуємо набори кнопок
'''
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [[button_join, button_cancel_record], [button_status, button_show], [button_open_sheet, button_clear_queue]],
    one_time_keyboard=False,  # Клавіатура залишається після використання
    resize_keyboard=True      # Клавіатура буде меншого розміру
)
'''
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [[button_join, button_cancel_record], [button_status, button_show], [button_open_sheet]],
    one_time_keyboard=False,  # Клавіатура залишається після використання
    resize_keyboard=True      # Клавіатура буде меншого розміру
)
CANCEL_KEYBOARD = ReplyKeyboardMarkup([[KeyboardButton(BUTTON_TEXT_CANCEL_OP)]], one_time_keyboard=True, resize_keyboard=True)

SHOW_OPTION_KEYBOARD = ReplyKeyboardMarkup([
        [KeyboardButton("Показати всі записи")],
        [KeyboardButton("Показати записи на конкретну дату")],
        [KeyboardButton(BUTTON_TEXT_CANCEL_OP)]],
        one_time_keyboard=True, resize_keyboard=True)

def get_ua_weekday(date_obj):
    weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Нд']
    return weekdays[date_obj.weekday()]

def calculate_date_probability(date_obj, dist):
    """
    Обчислює кумулятивну ймовірність того, що черга настане до кінця вказаної дати.
    Повертає ймовірність у відсотках (0-100).
    """
    try:
        ordinal = get_ordinal_date(date_obj)
        loc = dist['loc']
        scale = dist['scale']
        df = dist['df']
        # Обчислюємо кумулятивну ймовірність для цього порядкового номера (кінець дня)
        # Використовуємо ordinal + 1, оскільки ordinal представляє початок дня (або індекс цілого дня),
        # і ми хочемо отримати ймовірність того, що черга настане ДО кінця цього дня.
        prob = stats.t.cdf(ordinal + 1, df, loc=loc, scale=scale)
        return prob * 100
    except Exception as e:
        logger.error(f"Помилка обчислення ймовірності для {date_obj}: {e}")
        return 0.0

def calculate_end_date(start_date, days_count):
    """
    Обчислює кінцеву дату, додаючи вказану кількість робочих днів (Пн-Пт) до початкової дати.
    Це відтворює логіку, що використовується в date_keyboard для визначення останньої кнопки дати.
    """
    temp_date = start_date
    added = 0
    # Якщо початкова дата є робочим днем, вона враховується як перший день
    if temp_date.weekday() < 5:
        added = 1
    
    while added < days_count:
        temp_date += datetime.timedelta(days=1)
        if temp_date.weekday() < 5:
            added += 1
    return temp_date

def date_keyboard(today = datetime.date.today(), days_to_check = 0, days_ahead = 15, start_date=None, end_date=None, prediction_dist=None) -> object:
    # Генеруємо кнопки тільки для робочих днів
    flat_keyboard_buttons = []
    keyboard_buttons = []
    chunk_size = 3 
    
    current_check_date = today + datetime.timedelta(days=days_to_check)
    
    if start_date and end_date:
        # Переконуємось, що start_date не раніше current_check_date
        iter_date = max(current_check_date, start_date)
        limit_date = end_date
        
        # Генеруємо всі робочі дні в діапазоні
        while iter_date <= limit_date:
             if iter_date.weekday() < 5:
                 # Формуємо текст: "Пн: 25.12.25 (55%)" (день тижня, DD.MM.YY, %)
                 date_str = iter_date.strftime("%d.%m.%y")
                 weekday_str = get_ua_weekday(iter_date)
                 button_text = f"{weekday_str}: {date_str}"
                 
                 if prediction_dist:
                     percent = calculate_date_probability(iter_date, prediction_dist)
                     if percent >= 0.1:
                         button_text = f"{button_text} ({percent:.0f}%)"

                 flat_keyboard_buttons.append(KeyboardButton(button_text))
             iter_date += datetime.timedelta(days=1)
             # Запобіжник: перериваємо, якщо кнопок забагато
             if len(flat_keyboard_buttons) >= 30:
                 break
    else:
        buttons_added = 0
        iter_date = current_check_date
        while buttons_added < days_ahead:
            if iter_date.weekday() < 5: # Якщо це не субота (5) і не неділя (6)
                date_str = iter_date.strftime("%d.%m.%y")
                weekday_str = get_ua_weekday(iter_date)
                button_text = f"{weekday_str}: {date_str}"
                flat_keyboard_buttons.append(KeyboardButton(button_text))
                buttons_added += 1
            iter_date += datetime.timedelta(days=1)
    
    # Додаємо кнопку "Скасувати ввід" до клавіатури вибору дати
    keyboard_buttons.append([KeyboardButton(BUTTON_TEXT_CANCEL_OP)])
    # Улаштовуємо кнопки в chunk_size стовпчиків
    keyboard_buttons = [flat_keyboard_buttons[i:i + chunk_size] for i in range (0, len(flat_keyboard_buttons), chunk_size)]
    keyboard_buttons.append([button_cancel_op]) # додаємо в кінець кнопку /cancel
    return ReplyKeyboardMarkup(keyboard_buttons, one_time_keyboard=True, resize_keyboard=True)    


# --- ДОПОМІЖНА ФУНКЦІЯ ПЕРЕВІРКИ АДМІНІСТРАТОРА ---

def is_admin(user_id: int) -> bool:
    """Перевіряє, чи є користувач адміністратором."""
    return user_id in ADMIN_IDS
    
def is_banned(user_id: int) -> bool:
    """Перевіряє, чи забанений користувач."""
    return user_id in BANLIST

# --- ДОПОМІЖНА ФУНКЦІЯ ДАНИХ КОРИСТУВАЧА ---
def get_user_log_info(user: object) -> str:
    """
    Повертає рядок з інформацією про користувача для журналу,
    враховуючи можливу відсутність username або повного імені.
    """
    user_info = f"ID: {user.id}"
    if user.username:
        user_info += f", @{user.username}"
    elif user.full_name:
        user_info += f", Ім'я: {user.full_name}"
    else:
        user_info += ", Невідоме ім'я" # Малоймовірно, але для повноти
    return user_info

def get_user_telegram_data(user: object) -> dict:
    """
    Повертає словник з даними користувача Telegram для запису в DataFrame.
    """
    return {
        'TG ID': user.id,
        'TG Name': user.username if user.username else '',
        'TG Full Name': user.full_name if user.full_name else ''
    }

# --- ДОПОМІЖНА ФУНКЦІЯ ПЕРЕВІРКИ ID КОРИСТУВАЧА НА ПРОПУСК ЧЕРГИ ---
async def check_id_for_queue(user_id_to_check: int, user_prev_date: str, user_last_status: str):
    """
    Перевіряє, чи має користувач право на запис в чергу згідно з номерами проходження.
    Повертає (can_register: bool, message: str).
    """
    stats_df = await get_stats_data()
    if stats_df is None or stats_df.empty:
        return False, "Виникла помилка при перевірці даних, спробуйте пізніше."

    # Отримуємо максимальний номер і дату
    max_id_row = stats_df.loc[stats_df['Останній номер що зайшов'].idxmax()]
    max_id = max_id_row['Останній номер що зайшов']
    max_id_date = max_id_row['Дата прийому']

    # 1. Перевіряємо, чи ID більший за максимальний
    if user_id_to_check >= max_id:
        return True, ""

    # 2. Перевіряємо дату
    next_working_day = datetime.date.today() + datetime.timedelta(days=1)
    while next_working_day.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        next_working_day += datetime.timedelta(days=1)
    act_working_day = datetime.date.today()
    while act_working_day.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        act_working_day -= datetime.timedelta(days=1)
    try:
        prev_date = datetime.datetime.strptime(user_prev_date, "%d.%m.%Y").date()
    except:
        None
    else:
        if prev_date > act_working_day and user_last_status in ['Ухвалено']:
            return True, ""

    # 3. Шукаємо найближчий більший ID і перевіряємо запізнення
    stats_df['Cum_Max'] = stats_df['Останній номер що зайшов'][::-1].cummax()[::-1]
    filtered_df = stats_df[stats_df['Cum_Max'] > user_id_to_check]
    delay_days = filtered_df.shape[0]
    if delay_days <= 1:
        return True, "До вас застосовано `п.8` правил:\nВи пропустили свою чергу на один день.\nУ вас лишається `Остання спроба`.\n"
    else:
        return False, f"`Ви пропустили свою чергу!`\nКількість пропущених днів: `{delay_days}`.\nЗапис на наступні дні неможливий.\nЯкщо вас немає в списку відвідання на завтра, тоді для проходження ВЛК запишіться в кінець паперової черги і створіть новий запис знову. Нам дуже прикро."  
    # Якщо ID менше за припустимий, але немає відповідної дати
    return True, ""

# --- ДОПОМІЖНА ФУНКЦІЯ НАДСИЛАННЯ СПОВІЩЕННЯ В ГРУПУ ---
async def send_group_notification(context, message) -> None:
    global is_bot_in_group
    # Перевіряємо прапорець перед спробою надсилання
    if not is_bot_in_group:
        logger.warning("Сповіщення не надіслано в груповий чат, бот був вилучений з нього.")
        return
    try:
        await context.bot.send_message(chat_id=GROUP_ID, text=message, parse_mode="HTML")
        logger.info("Сповіщення успішно надіслано в груповий чат.")
    except telegram.error.Forbidden as e:
        # 2. Якщо виникла помилка Forbidden, ми знаємо, що бота вилучили.
        is_bot_in_group = False
        logger.error(f"Помилка: бот був вилучений з чату ID {GROUP_ID}. Сповіщення вимкнено.")
    except telegram.error.TelegramError as e:
        # 3. Обробляємо інші помилки Telegram API, якщо вони виникнуть
        logger.error(f"Інша помилка Telegram API при надсиланні сповіщення: {e}")

# --- Допоміжна функція для надсилання повідомлення користувачу ---
async def send_user_notification(context: ContextTypes.DEFAULT_TYPE, user_tg_id: str, message: str) -> None:
    """Надсилає особисте повідомлення користувачу за його TG ID."""
    if user_tg_id != '':
        try:
            await context.bot.send_message(chat_id=user_tg_id, text=message, parse_mode="HTML", reply_markup=MAIN_KEYBOARD)
            logger.info(f"Особисте сповіщення успішно надіслано користувачу {user_tg_id}.")
        except Exception as e:
            logger.error(f"Помилка при надсиланні особистого сповіщення користувачу {user_tg_id}: {e}")

# --- ДОПОМІЖНА ФУНКЦІЯ ДЛЯ ВІДОБРАЖЕННЯ ЧЕРГИ (З ПАГІНАЦІЄЮ) ---
async def display_queue_data(update: Update, data_frame: pd.DataFrame, title: str = "Поточна черга:", reply_markup = None, iConfirmation = False) -> None:
    # Фільтруємо лише актуальні записи (де 'Дата' не порожня і є останнім записом для даного ID)
    # Спочатку перетворюємо стовпець "Змінено" на datetime для коректного сортування
    temp_df = data_frame.copy()
    # Перевірка на пустий 'Змінено' перед перетворенням
    temp_df['Змінено_dt'] = pd.to_datetime(temp_df['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
    temp_df['Змінено_dt'] = temp_df['Змінено_dt'].fillna("01.01.2025 00:00:00")
    #temp_df = temp_df.dropna(subset=['Змінено_dt'])


    # Сортуємо за ID та часом зміни (найновіші записи будуть останніми для кожного ID)
    temp_df_sorted = temp_df.sort_values(by=['ID', 'Змінено_dt'], ascending=[True, True])

    # Вибираємо тільки останній запис для кожного ID
    actual_records = temp_df_sorted.drop_duplicates(subset='ID', keep='last')

    # Фільтруємо ті, у яких поле "Дата" не порожнє (актуальні записи)
    actual_queue = actual_records[
        (actual_records['Дата'].astype(str).str.strip() != '') &
        (actual_records['Статус'].astype(str).str.strip().str.lower() == 'ухвалено')
    ].copy()

    if actual_queue.empty:
        await update.message.reply_text(f"{title}\nЧерга порожня або жоден запис ще не ухвалено. Гарна нагода записатися!", reply_markup=reply_markup) # Додаємо клавіатуру)
        return

    # Сортуємо актуальні записи за датою для відображення
    try:
        # Створюємо тимчасовий стовпець для сортування, щоб не змінювати початковий DataFrame
        current_date_obj = datetime.date.today()
        actual_queue['Дата_dt'] = pd.to_datetime(actual_queue['Дата'].astype(str), format="%d.%m.%Y", dayfirst=True, errors='coerce')
        actual_queue = actual_queue.dropna(subset=['Дата_dt']) # Видаляємо записи з некоректними датами

        sorted_df_for_display = actual_queue.sort_values(by=['Дата_dt', 'ID'], ascending=[True, True]).loc[actual_queue['Дата_dt'].dt.date >= current_date_obj].drop(columns=['Дата_dt', 'Змінено_dt'])
    except Exception as e: # На випадок, якщо у файлі є некоректні дати
        logger.error(f"Помилка сортування черги для відображення: {e}. Сортування без перетворення дат.")
        sorted_df_for_display = actual_queue.sort_values(by=['Дата', 'ID'], ascending=[True, True]).drop(columns=['Змінено_dt']) # Сортуємо як рядок

    # Формуємо список рядків черги
    queue_lines = []
    # Для відображення показуємо лише ID та Дату
    if iConfirmation:
        last_known_state = load_status_state()       
        for index, row in sorted_df_for_display.iterrows():
            last_status_info = last_known_state.get(row['ID'])
            queue_lines.append(f"**{len(queue_lines) + 1}.** ID: `{row['ID']}`, Дата: `{row['Дата']}`, `{last_status_info['confirmation']}`")
    else:    
        for index, row in sorted_df_for_display.iterrows():
            queue_lines.append(f"**{len(queue_lines) + 1}.** ID: `{row['ID']}`, Дата: `{row['Дата']}`")
    base_queue_text = f"📊 **{title} {sorted_df_for_display.shape[0]} записів**\n"
    current_message_parts = [base_queue_text]
    current_part_length = len(base_queue_text)
    MAX_MESSAGE_LENGTH = 1500 # Має бути менше 4096, обираємо 1500 щоб мати запас на форматування

    for line in queue_lines:
        # Перевіряємо, чи додавання нового рядка не перевищить ліміт
        if current_part_length + len(line) + 1 > MAX_MESSAGE_LENGTH: # +1 для нового рядка \n
            # Відправляємо поточну частину
            await update.message.reply_text(
                current_message_parts[-1], parse_mode='Markdown', reply_markup=reply_markup
            )
                # Починаємо нову частину
            current_message_parts.append(line)
            current_part_length = len(line)
        else:
            # Додаємо рядок до поточної частини
            if len(current_message_parts) == 1: # Якщо це перша частина, додаємо до base_queue_text
                current_message_parts[0] += f"\n{line}"
            else: # Якщо це наступні частини, просто додаємо рядок
                current_message_parts[-1] += f"\n{line}"
            current_part_length += len(line) + 1 # +1 для нового рядка \n

    # Відправляємо останню частину, якщо вона не порожня
    if current_message_parts[-1]:
        await update.message.reply_text(
            current_message_parts[-1], parse_mode='Markdown', reply_markup=reply_markup
            # Додаємо клавіатуру
        )

# --- ФУНКЦІЇ ОБРОБНИКІВ КОМАНД ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.type == 'private':
        user = update.effective_user
        logger.info(f"Користувач {get_user_log_info(user)} розпочав розмову.")
        
        caption_text = (
            f"Вітаю, {user.mention_html()}\n"
            "Я бот для запису в електронну чергу ВЛК на Закревського, 81/1\n"
            "1. Ознайомтеся з інфографікою 👆\n"
            "2. Оберайте потрібну команду за допомогою кнопок:\n"
            "* <code>Записатися / Перенести</code> - записатися або перенести дату відвідання\n"
            "* <code>Скасувати запис</code> - скасувати свій запис\n"
            "* <code>Переглянути чергу</code> - переглянути поточну чергу повністю або на обраний день\n"
            "* <code>Відкрити таблицю</code> - перейти до таблиці Google Sheets з даними черги (тільки для адміністраторів)\n"
            #"<code>Очистити чергу</code> - очистити чергу (тільки для адміністраторів)\n"
            "* <code>Скасувати ввід</code> - скасувати ввід під час діалогу"
        )

        try:
            with open('infographic.jpg', 'rb') as photo:
                await update.message.reply_photo(
                    photo=photo,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=MAIN_KEYBOARD
                )
        except Exception as e:
            logger.error(f"Не вдалося надіслати фото (infographic.jpg): {e}")
            # Fallback to text only if image fails
            await update.message.reply_html(
                caption_text,
                reply_markup=MAIN_KEYBOARD,
            )

    # Функція, яка містить основну логіку очищення
async def perform_queue_cleanup(logger_info_prefix: str = "Очищення за розкладом"):
    """
    Виконує логіку очищення черги. Може бути викликана як з команди, так і за розкладом.
    """
    global queue_df   
    # Для запланованого завдання немає об'єкта user, тому використовуємо загальний лог
    logger.info(f"{logger_info_prefix}: Розпочато розумне очищення черги.")

    # 1. Завантажуємо актуальний стан черги
    queue_df = load_queue_data()
    if queue_df is None: # Перевіряємо, чи була помилка завантаження
        logger.error(f"{logger_info_prefix}: Не вдалося завантажити чергу для очищення. Можливо, проблема зі зв'язком з Google Sheets.")
        return -1 # Повертаємо -1, щоб сигналізувати про помилку
    sort_df = queue_df.copy()
    if sort_df.empty:
        logger.info(f"{logger_info_prefix}: Черга вже порожня.")
        return 0 # Повертаємо 0, якщо черга порожня (не помилка)

    initial_records_count = len(sort_df)

    # 2. Підготовка допоміжних стовпців для фільтрації
    sort_df['Статус_clean'] = sort_df['Статус'].astype(str).str.strip().str.lower()
    sort_df['Дата_clean'] = sort_df['Дата'].astype(str).str.strip()
    # Перетворюємо стовпець 'Дата' на формат datetime для порівняння
    sort_df['Дата_dt'] = pd.to_datetime(sort_df['Дата_clean'], format="%d.%m.%Y", dayfirst=True, errors='coerce')
    sort_df['Змінено_clean'] = sort_df['Змінено'].astype(str).str.strip()
    sort_df['Змінено_dt'] = pd.to_datetime(sort_df['Змінено_clean'], format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
    # Створюємо список видалення
    current_date_obj = datetime.date.today() # Поточна дата
    unique_ids = sort_df['ID'].unique()
    index_to_drop = []
    index_to_drop.extend(sort_df.loc[(sort_df['Дата_dt'].dt.date < current_date_obj) & (sort_df['Дата_dt'].notna())].index.tolist())
  
    for cur_id in unique_ids:
        max_mod_idx = sort_df[sort_df['ID'] == cur_id]['Змінено_dt'].idxmax()
        TG_ID = sort_df['TG ID'][max_mod_idx].strip()
        index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & ((sort_df['Дата_dt'].dt.date >= current_date_obj) | (sort_df['Дата_dt'].isna())) & (sort_df['Статус_clean'].isin(['відхилено']))].index.tolist())
        if  sort_df['Статус_clean'][max_mod_idx] == 'ухвалено':
            index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] == TG_ID)].index.tolist())
            index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] != TG_ID) & (sort_df['Дата_dt'].isna())].index.tolist())
            if pd.notna(sort_df['Дата_dt'][max_mod_idx]):
                if sort_df['Дата_dt'].dt.date[max_mod_idx] < current_date_obj:
                    index_to_drop.extend(sort_df.loc[(sort_df['ID'] == cur_id) & (sort_df['Змінено_dt'] < sort_df['Змінено_dt'][max_mod_idx]) & (sort_df['Статус_clean'].isin(['на розгляді', 'ухвалено'])) & (sort_df['TG ID'] != TG_ID) & (sort_df['Дата_dt'].dt.date >= current_date_obj)].index.tolist())
                    
    unique_index_to_drop = list(set(index_to_drop))
    records_to_keep = sort_df.drop(index=unique_index_to_drop).copy()
    
    # Видаляємо допоміжні стовпці перед збереженням
    if 'Статус_clean' in records_to_keep.columns:
        records_to_keep = records_to_keep.drop(columns=['Статус_clean'])
    if 'Дата_clean' in records_to_keep.columns:
        records_to_keep = records_to_keep.drop(columns=['Дата_clean'])
    if 'Дата_dt' in records_to_keep.columns:
        records_to_keep = records_to_keep.drop(columns=['Дата_dt'])
    if 'Змінено_dt' in records_to_keep.columns:
        records_to_keep = records_to_keep.drop(columns=['Змінено_dt'])
    if 'Змінено_clean' in records_to_keep.columns:
        records_to_keep = records_to_keep.drop(columns=['Змінено_clean'])

    # Оновлюємо глобальний DataFrame
    queue_df = records_to_keep
    # 3. Зберігаємо оновлений DataFrame у Google Sheet
    if not save_queue_data_full(queue_df): # Перевіряємо результат збереження
        logger.error(f"{logger_info_prefix}: Помилка при збереженні очищеної черги в Google Sheet.")
        return -1 # Повертаємо -1, щоб сигналізувати про помилку

    records_removed_count = initial_records_count - len(queue_df)

    logger.info(f"{logger_info_prefix}: Очищено {records_removed_count} записів. Залишилось {len(queue_df)} записів.")
    return records_removed_count

'''
async def clear_queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Очищує чергу, залишаючи:
    1. Всі записи з майбутньою/поточною датою.
    2. Записи з порожньою датою, що все ще знаходяться в статусі "На розгляді" або "Відхилено".

    Видаляє:
    1. Записи, що є старішими за поточну дату (поле "Дата").
    2. Записи з порожньою датою та статусом "Ухвалено".
    """
    global queue_df
    user = update.effective_user
    user_id = user.id

    if not is_admin(user_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував очистити чергу.")
        await update.message.reply_text(
            "У вас недостатньо прав для виконання цієї команди.",
            reply_markup=MAIN_KEYBOARD
        )
        return

    logger.info(f"Адміністратор {get_user_log_info(user)} розпочав розумне очищення черги вручну.")
    
    # Викликаємо основну логіку очищення
    removed_count = await perform_queue_cleanup(logger_info_prefix=f"Ручне очищення (адмін {get_user_log_info(user)})")

    if removed_count == -1: # Перевіряємо, чи була помилка під час очищення/збереження
        await update.message.reply_text(
            "Сталася помилка під час очищення черги або її збереження. Будь ласка, спробуйте ще раз пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
    else:
        await update.message.reply_text(
            f"Черга очищена. Видалено {removed_count} неактуальних записів. Залишилось {len(queue_df)} записів.",
            reply_markup=MAIN_KEYBOARD
        )
'''
async def open_sheet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Відправляє користувачу посилання на Google Sheet."""
    
    user = update.effective_user
    user_id = user.id
    
    if not is_admin(user_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував отримати посилання на таблицю черги Google.")
        await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
        return
    # Перевіряємо, чи SHEETS_SERVICE ініціалізовано.
    if SHEETS_SERVICE is None:
        logger.error(f"Адміністратор {get_user_log_info(user)} спробував отримати посилання, але Google Sheets API не ініціалізовано.")
        await update.message.reply_text(
            "Не вдалося отримати посилання на таблицю, оскільки сервіс Google Sheets не ініціалізовано. Будь ласка, повідомте адміністратора бота.",
            reply_markup=MAIN_KEYBOARD
        )
        return
        
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
    logger.info(f"Користувач {get_user_log_info(user)} отримав посилання на Google Sheet.")
    await update.message.reply_text(
        f"Ось посилання на Google Таблицю з даними черги:\n{sheet_url}",
        reply_markup=MAIN_KEYBOARD
    )

# --- ФУНКЦІЇ ДЛЯ КЕРУВАННЯ АДМІНІСТРАТОРАМИ ---

async def grant_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Додає користувача до списку адміністраторів."""
    user = update.effective_user
    requester_id = user.id

    if not is_admin(requester_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував додати адміністратора.")
        await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
        return
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача, якого ви хочете додати до адміністраторів. "
            "Наприклад: `/grant_admin 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        new_admin_id = int(context.args[0])
        if new_admin_id in ADMIN_IDS:
            await update.message.reply_text(
                f"Користувач з ID `{new_admin_id}` вже є адміністратором.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        ADMIN_IDS.append(new_admin_id)
        config['BOT_SETTINGS']['ADMIN_IDS'] = ','.join(map(str, ADMIN_IDS))
        save_config() # Зберігаємо зміни у config.ini

        logger.info(f"Адміністратор {get_user_log_info(user)} додав нового адміністратора: ID {new_admin_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{new_admin_id}` успішно доданий до списку адміністраторів.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        logger.warning(f"Адміністратор {get_user_log_info(user)} ввів некоректний ID для grant_admin: '{context.args[0]}'")
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ціле число.",
            reply_markup=MAIN_KEYBOARD
        )
    except Exception as e:
        logger.error(f"Помилка при додаванні адміністратора: {e}")
        await update.message.reply_text(
            "Сталася помилка при додаванні адміністратора.",
            reply_markup=MAIN_KEYBOARD
        )

async def drop_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Видаляє користувача зі списку адміністраторів."""
    user = update.effective_user
    requester_id = user.id

    if not is_admin(requester_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував видалити адміністратора.")
        await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
        return
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача, якого ви хочете видалити з адміністраторів. "
            "Наприклад: `/drop_admin 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        admin_to_remove_id = int(context.args[0])
        
        if admin_to_remove_id == requester_id:
            await update.message.reply_text(
                "Ви не можете видалити самого себе з адміністраторів. Попросіть іншого адміністратора це зробити.",
                reply_markup=MAIN_KEYBOARD
            )
            return

        if admin_to_remove_id not in ADMIN_IDS:
            await update.message.reply_text(
                f"Користувач з ID `{admin_to_remove_id}` не є адміністратором.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        ADMIN_IDS.remove(admin_to_remove_id)
        config['BOT_SETTINGS']['ADMIN_IDS'] = ','.join(map(str, ADMIN_IDS))
        save_config() # Зберігаємо зміни у config.ini

        logger.info(f"Адміністратор {get_user_log_info(user)} видалив адміністратора: ID {admin_to_remove_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{admin_to_remove_id}` успішно видалений зі списку адміністраторів.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        logger.warning(f"Адміністратор {get_user_log_info(user)} ввів некоректний ID для drop_admin: '{context.args[0]}'")
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ціле число.",
            reply_markup=MAIN_KEYBOARD
        )
    except Exception as e:
        logger.error(f"Помилка при видаленні адміністратора: {e}")
        await update.message.reply_text(
            "Сталася помилка при видаленні адміністратора.",
            reply_markup=MAIN_KEYBOARD
        )
        
# --- ФУНКЦІЇ ДЛЯ КЕРУВАННЯ СПИСКОМ ЗАБЛОКОВАНИХ ---

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Додає користувача до списку заблокованих."""
    user = update.effective_user
    requester_id = user.id

    if not is_admin(requester_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував розширити список заблокованих.")
        await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
        return
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача, якого ви хочете додати до списку заблокованих. "
            "Наприклад: `/ban 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        new_ban_id = int(context.args[0])
        if new_ban_id in BANLIST:
            await update.message.reply_text(
                f"Користувач з ID `{new_ban_id}` вже є в списку заблокованих.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        BANLIST.append(new_ban_id)
        config['BOT_SETTINGS']['BANLIST'] = ','.join(map(str, BANLIST))
        save_config() # Зберігаємо зміни у config.ini

        logger.info(f"Адміністратор {get_user_log_info(user)} заблокував користувача: ID {new_ban_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{new_ban_id}` успішно доданий до списку заблокованих.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        logger.warning(f"Адміністратор {get_user_log_info(user)} ввів некоректний ID для списку заблокованих: '{context.args[0]}'")
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ідентифікатор користувача телеграм (TG ID).",
            reply_markup=MAIN_KEYBOARD
        )
    except Exception as e:
        logger.error(f"Помилка при розширенні списку заблокованих: {e}")
        await update.message.reply_text(
            "Сталася помилка при блокуванні користувача.",
            reply_markup=MAIN_KEYBOARD
        )

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Видаляє користувача зі списку заблокованих."""
    user = update.effective_user
    requester_id = user.id

    if not is_admin(requester_id):
        logger.warning(f"Користувач {get_user_log_info(user)} без прав адміністратора спробував скоротити список заблокованих.")
        await update.message.reply_text("У вас недостатньо прав для виконання цієї команди.", reply_markup=MAIN_KEYBOARD)
        return
    
    if not context.args:
        await update.message.reply_text(
            "Будь ласка, вкажіть ID користувача, якого ви хочете видалити зі списку заблокованих. "
            "Наприклад: `/unban 123456789`",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        return

    try:
        unban_id = int(context.args[0])
        
        if unban_id == requester_id:
            await update.message.reply_text(
                "Ви не можете видалити самого себе з списку заблокованих. Попросіть іншого адміністратора це зробити.",
                reply_markup=MAIN_KEYBOARD
            )
            return

        if unban_id not in BANLIST:
            await update.message.reply_text(
                f"Користувач з ID `{unban_id}` відсутній в списку заблокованих.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
            return

        BANLIST.remove(unban_id)
        config['BOT_SETTINGS']['BANLIST'] = ','.join(map(str, BANLIST))
        save_config() # Зберігаємо зміни у config.ini

        logger.info(f"Адміністратор {get_user_log_info(user)} видалив користувача зі списку заблокованих: ID {unban_id}.")
        await update.message.reply_text(
            f"Користувач з ID `{unban_id}` успішно видалений зі списку заблокованих.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
    except ValueError:
        logger.warning(f"Адміністратор {get_user_log_info(user)} ввів некоректний ID для видалення зі списку заблокованих: '{context.args[0]}'")
        await update.message.reply_text(
            "Невірний формат ID. Будь ласка, введіть ідентифікатор користувача телеграм (TG ID).",
            reply_markup=MAIN_KEYBOARD
        )
    except Exception as e:
        logger.error(f"Помилка при скороченні списку заблокованих: {e}")
        await update.message.reply_text(
            "Сталася помилка при розблокуванні користувача.",
            reply_markup=MAIN_KEYBOARD
        )

# --- ФУНКЦІЇ ДЛЯ РОЗМОВИ ЗАПИСУ В ЧЕРГУ (BUTTON_TEXT_JOIN) ---

async def join_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес запису в чергу, просячи користувача ввести ID."""
    if is_banned(update.effective_user.id):
        logger.warning(f"Заблокований користувач {get_user_log_info(update.effective_user)} намагався створити новий запис.")
        await update.message.reply_text(
            "Ваш обліковвй запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову 
        
    global queue_df # Оновлюємо DataFrame перед початком діалогу
    queue_df = load_queue_data()
    if queue_df is None: # Перевірка на помилку завантаження
        logger.error(f"Помилка завантаження даних для запису в чергу або перенесення дати відвідування користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову    

    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав запис/перенесення.")
    # Зберігаємо дані користувача в context.user_data для подальшого використання
    context.user_data['telegram_user_data'] = get_user_telegram_data(update.effective_user)
    await update.message.reply_text(
        "Будь ласка, введіть свій номер в списку первинної черги. Це може бути ціле число (наприклад, `9999`) "
        "або два цілих числа, розділені слешем (наприклад, `9999/1`). "
        "Цей номер надалі буде вашим ID в черзі.",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD # Можна використовувати ForceReply для введення
    )
    return JOIN_GETTING_ID

async def join_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID від користувача, перевіряє формат і просить дату. Якщо ID існує, готує його до оновлення."""
    #global queue_df
    user_id_input = update.message.text.strip()

    # Регулярний вираз для перевірки формату ID
    id_pattern = r"^(\d+|\d+\/\d+)$"
    
    if not re.match(id_pattern, user_id_input):
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID: '{user_id_input}'")
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return JOIN_GETTING_ID # Залишаємося в тому ж стані

    # Перевіряємо, чи ID вже існує
    context.user_data['temp_id'] = user_id_input
    # Очищаємо попередній стан попереджень та прогнозів при введенні нового ID
    context.user_data.pop('warning_shown', None)
    context.user_data.pop('prediction_bounds', None)
    
    # Знаходимо останній актуальний запис для цього ID
    temp_df_for_prev = queue_df.copy()
    temp_df_for_prev['Змінено_dt'] = pd.to_datetime(temp_df_for_prev['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
    temp_df_for_prev['Змінено_dt'] = temp_df_for_prev['Змінено_dt'].fillna("01.01.2025 00:00:00")

    last_record_for_id = temp_df_for_prev[(temp_df_for_prev['ID'] == user_id_input) & (temp_df_for_prev['Статус'] == 'Ухвалено')].sort_values(by='Змінено_dt', ascending=False)
    
    previous_date = ''
    if not last_record_for_id.empty:
        # Беремо останній запис і його дату, якщо вона не порожня
        last_date = last_record_for_id.iloc[0]['Дата']
        last_note = last_record_for_id.iloc[0]['Примітки']
        last_status = last_record_for_id.iloc[0]['Статус']
        if pd.isna(last_date) or last_date == '': # Перевіряємо на NaN або порожній рядок
            previous_date = '' # Якщо останній запис був скасований або порожній, то попередньої дати немає
        else:
            previous_date = last_date # Зберігаємо попередню дату
            
        context.user_data['previous_state'] = previous_date # Зберігаємо для майбутнього запису
        context.user_data['user_notes'] = last_note # Зберігаємо для майбутнього запису
        # Повідомляємо користувача, що запис буде оновлено
        await update.message.reply_text(
            f"Номер `{user_id_input}` вже записаний в черзі.\nВаш попередній запис {'на дату' if previous_date else ''} `{previous_date if previous_date else 'Скасовано'}` буде оновлено.",
            parse_mode='Markdown'
        )
    else:
        last_status = ''
        context.user_data['previous_state'] = '' # Якщо ID новий, попередній стан порожній
        await update.message.reply_text(
            f"Ваш номер `{user_id_input}` прийнято. ",
            parse_mode='Markdown'
        )
    can_register, user_warning = await check_id_for_queue(extract_main_id(user_id_input), context.user_data['previous_state'], last_status)
    # backdoor for admins
    if is_admin(update.effective_user.id):
        can_register = True  
        user_warning = ''  
    if can_register:
        today = datetime.date.today()
        
        # --- ЛОГІКА ПРОГНОЗУВАННЯ ---
        stats_df = await get_stats_data()
        prediction = calculate_prediction(extract_main_id(user_id_input), stats_df)
        
        prediction_text = ""
        if prediction:
            context.user_data['prediction_bounds'] = prediction
            dist = prediction['dist']
            
            # Обчислюємо ймовірності для меж діапазону
            try:
                # Визначаємо фактичну дату початку (враховуючи 'завтра' та вихідні)
                start_date_candidate = prediction['mean']
                min_date = today + datetime.timedelta(days=1)
                
                actual_start_date = max(start_date_candidate, min_date)
                
                # Переходимо до наступного робочого дня, якщо потрібно
                while actual_start_date.weekday() >= 5:
                     actual_start_date += datetime.timedelta(days=1)
                
                # Якщо початкова дата ПІЗНІШЕ кінцевої дати (наприклад, малий ID, прогноз у минулому),
                # ми повинні просто показати наступні N доступних днів, починаючи з завтра
                if actual_start_date > prediction['h90']:
                     actual_start_date = min_date
                     while actual_start_date.weekday() >= 5:
                         actual_start_date += datetime.timedelta(days=1)
                     # Примусово встановлюємо кінцеву дату None, щоб date_keyboard згенерувала N днів наперед
                     calc_end_date = None
                else:
                     calc_end_date = prediction['h90']

                prob_start = calculate_date_probability(actual_start_date, dist)
                if calc_end_date:
                    prob_h90 = calculate_date_probability(calc_end_date, dist)
                    end_date_str = f"`{calc_end_date.strftime('%d.%m.%Y')}` ({prob_h90:.0f}%)"
                else:
                    est_end_date = calculate_end_date(actual_start_date, days_ahead)
                    prob_end = calculate_date_probability(est_end_date, dist)
                    end_date_str = f"`{est_end_date.strftime('%d.%m.%Y')}` ({prob_end:.0f}%)"

                range_info = f"`{actual_start_date.strftime('%d.%m.%Y')}` ({prob_start:.0f}%) - {end_date_str}"
            except Exception as e:
                logger.error(f"Помилка обчислення ймовірностей діапазону: {e}")
                range_info = f"`{prediction['mean'].strftime('%d.%m.%Y')}` - `{prediction['h90'].strftime('%d.%m.%Y')}`"
                calc_end_date = prediction['h90']

            # Відображаємо діапазон
            DATE_KEYBOARD = date_keyboard(today, 1, days_ahead, start_date=actual_start_date, end_date=calc_end_date, prediction_dist=prediction.get('dist'))
            
            # Додаємо інформацію до повідомлення
            prediction_text = (
                f"{range_info}. *Відсоток означає ймовірність того, що ви зможете почати ВЛК в цей день.*"
            )
        else:
            context.user_data.pop('prediction_bounds', None)
            DATE_KEYBOARD = date_keyboard(today, 1, days_ahead)

        if user_warning != '':
            context.user_data['user_notes'] = 'Остання спроба'
        
        await update.message.reply_text(
            f"{'УВАГА: '+user_warning if user_warning != '' else ''}"
            f"Виберіть бажану дату запису. Ви можете обрати одну з рекомендованих дат: {prediction_text}\n\n"
            f"Або введіть дату з клавіатури. Дата повинна бути в форматі `ДД.ММ.РРРР`, пізнішою за поточну (`{today.strftime('%d.%m.%Y')}`) та бути робочим днем (Понеділок - П'ятниця).",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD # Використовуємо клавіатуру для дати
        )
        return JOIN_GETTING_DATE
    else:
        await update.message.reply_text(
            f"{user_warning}",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD # Використовуємо основну клавіатуру
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END

async def join_get_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує дату від користувача, перевіряє її, оновлює або додає запис."""
    global queue_df
    date_input = update.message.text.strip()
    
    user_id = context.user_data.get('temp_id')
    previous_state = context.user_data.get('previous_state', '') # Отримуємо попередній стан
    user_notes = context.user_data.get('user_notes', '') # Отримуємо примітки
    telegram_user_data = context.user_data.get('telegram_user_data') # Отримуємо дані користувача

    # Використовуємо regex для пошуку дати, ігноруючи емодзі та відсотки
    # Оновлений regex для підтримки формату без року (або з роком) на кнопках, але користувач може ввести повну дату
    # Пріоритет: спочатку шукаємо повну дату dd.mm.yyyy або dd.mm.yy
    
    match_full = re.search(r'(\d{2})\.(\d{2})\.(\d{2,4})', date_input)
    
    try:
        if match_full:
            date_text = match_full.group(0)
            # Якщо рік має 2 цифри, strptime %y обробить його (як 20xx)
            if len(match_full.group(3)) == 2:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
            else:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
        else:
            # Спроба розпарсити формат з кнопки: "Вт: 20.01 (51%)" -> шукаємо DD.MM (старий формат, або якщо без року)
            match_short = re.search(r'(\d{1,2})\.(\d{1,2})', date_input)
            if match_short:
                day = int(match_short.group(1))
                month = int(match_short.group(2))
                
                # Визначаємо рік. Якщо місяць менший за поточний, це наступний рік
                # Але треба бути обережним, якщо поточний місяць грудень, а кнопка січень
                current_today = datetime.date.today()
                year = current_today.year
                
                # Проста евристика: якщо дата вже минула в цьому році, то це наступний рік
                candidate_date = datetime.date(year, month, day)
                if candidate_date < current_today:
                    year += 1
                    
                chosen_date = datetime.date(year, month, day)
            else:
                 # Fallback: пробуємо просто перше слово, якщо це не спрацювало раніше
                 date_text = date_input.split()[0]
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()

    except ValueError:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати: '{date_input}'")
        today = datetime.date.today() # Поточна дата
        DATE_KEYBOARD=date_keyboard(today, 1, days_ahead)
        await update.message.reply_html(
            "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code> (наприклад, 25.12.2025) або скасуйте дію.",
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE # Залишаємося в тому ж стані

    current_date_obj = datetime.date.today()

    # Перевірка, чи дата поточна або пізніша 
    if chosen_date <= current_date_obj:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату раніше ніж наступний робочий день: '{date_input}'")
        DATE_KEYBOARD=date_keyboard(current_date_obj, 1, days_ahead)
        await update.message.reply_text(
            f"Дата повинна бути пізнішою за поточну (`{current_date_obj.strftime('%d.%m.%Y')}`). Будь ласка, спробуйте ще раз або скасуйте дію.",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE
    
    # ПЕРЕВІРКА: чи є обрана дата вихідним днем (субота або неділя)
    if chosen_date.weekday() >= 5: # 5 - субота, 6 - неділя
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів вихідний день: '{date_input}'")
        DATE_KEYBOARD=date_keyboard(current_date_obj, 1, days_ahead)
        await update.message.reply_html(
            "Ви обрали вихідний день (Субота або Неділя). Будь ласка, оберіть <code>робочий день</code> (Понеділок - П'ятниця) або скасуйте дію.",
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE # Залишаємося в тому ж стані

    # ПЕРЕВІРКА: чи дата співпадає з поточною датою запису
    if date_input == previous_state:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату, що співпадає з попереднім записом: '{date_input}'")
        DATE_KEYBOARD=date_keyboard(current_date_obj, 1, days_ahead)
        await update.message.reply_text(
            f"Дата не повинна співпадати з поточною датою запису (`{chosen_date.strftime('%d.%m.%Y')}`). Будь ласка, оберіть іншу дату або скасуйте дію.",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD
        )
        return JOIN_GETTING_DATE # Залишаємося в тому ж стані        

    # --- ЛОГІКА ПОПЕРЕДЖЕНЬ ---
    prediction = context.user_data.get('prediction_bounds')
    warning_shown = context.user_data.get('warning_shown', False)
    warned_date_str = context.user_data.get('warned_date')

    if prediction:
        # Check if this is a re-confirmation of the SAME warned date
        if warning_shown and warned_date_str and warned_date_str == chosen_date.strftime("%d.%m.%Y"):
                # User confirmed the warning by re-entering the same date
                pass 
        else:
                # Evaluate warning for the new date (or if warning wasn't shown yet)
            warn_msg = None
            
            # Calculate probability for chosen date
            try:
                dist = prediction['dist']
                chosen_ord = get_ordinal_date(chosen_date)
                chosen_prob = stats.t.cdf(chosen_ord + 1, dist['df'], loc=dist['loc'], scale=dist['scale']) * 100
            except Exception as e:
                logger.error(f"Error calculating chosen date probability: {e}")
                chosen_prob = 0
                
            if chosen_date < prediction['mean']:
                # Показуємо попередження лише якщо обрана ймовірність дійсно низька (наприклад, < 50%)
                # Якщо prediction['mean'] у минулому, chosen_prob все одно може бути високою (наприклад, 100%)
                if chosen_prob < 50:
                    try:
                        prob_mean = calculate_date_probability(prediction['mean'], dist)
                        prob_h90 = calculate_date_probability(prediction['h90'], dist)
                        
                        range_info = f"`{prediction['mean'].strftime('%d.%m.%Y')}` ({prob_mean:.0f}%) - `{prediction['h90'].strftime('%d.%m.%Y')}` ({prob_h90:.0f}%)"
                    except Exception as e:
                        logger.error(f"Помилка обчислення ймовірностей діапазону для попередження: {e}")
                        range_info = f"`{prediction['mean'].strftime('%d.%m.%Y')}` - `{prediction['h90'].strftime('%d.%m.%Y')}`"

                    warn_msg = (
                        f"⚠️ *Попередження:* Для обраної дати `{chosen_date.strftime('%d.%m.%Y')}` ви маєте *низьку ймовірність* почати ВЛК ({chosen_prob:.0f}%).\n"
                        f"Рекомендовано обирати дату з інтервалу {range_info}."
                    )
            elif chosen_date > prediction['h90']:
                # Якщо прогнозована "безпечна" дата (h90) в минулому або дуже скоро,
                # вибір дати трохи в майбутньому (наприклад, в межах стандартного діапазону кнопок) не повинен викликати попередження.
                # Ми перевіряємо, чи є обрана дата невиправдано далекою відносно стандартного вікна.
                # Стандартне вікно - це те, що показується на кнопках (days_ahead робочих днів).
                
                # Починаємо від "завтра" (або від наступного робочого дня)
                current_start = datetime.date.today() + datetime.timedelta(days=1)
                while current_start.weekday() >= 5:
                    current_start += datetime.timedelta(days=1)
                
                # Використовуємо ту саму логіку, що й для кнопок, щоб знайти кінець стандартного вікна
                standard_window_end = calculate_end_date(current_start, days_ahead)
                
                threshold_date = max(prediction['h90'], standard_window_end)

                if chosen_date > threshold_date:
                    # Визначаємо дату для прикладу в попередженні.
                    # Якщо h90 в минулому, використовуємо "завтра" (або наступний робочий день) як більш релевантний приклад.
                    example_date = prediction['h90']
                    if example_date < current_start:
                        example_date = current_start # current_start вже враховує вихідні і починається від завтра

                    try:
                        example_prob = calculate_date_probability(example_date, dist)
                        example_prob_str = f"{example_prob:.0f}%"
                    except Exception as e:
                            example_prob_str = ""

                    warn_msg = (
                        f"⚠️ *Попередження:* Обрана дата `{chosen_date.strftime('%d.%m.%Y')}` *занадто далеко в майбутньому*. "
                        f"Вам не треба так довго чекати, шанс успішно почати ВЛК майже гарантований для ближчих дат (наприклад {example_prob_str} для `{example_date.strftime('%d.%m.%Y')}`)."
                    )
                
            if warn_msg:
                context.user_data['warning_shown'] = True
                context.user_data['warned_date'] = chosen_date.strftime("%d.%m.%Y")
                
                today = datetime.date.today()
                
                # Determine the actual start date for the recommended range (max of mean prediction or tomorrow)
                start_date_candidate = prediction['mean']
                min_date = today + datetime.timedelta(days=1)
                actual_start_date = max(start_date_candidate, min_date)
                while actual_start_date.weekday() >= 5:
                        actual_start_date += datetime.timedelta(days=1)

                # If start date is > end date, we need to fallback to N days logic
                if actual_start_date > prediction['h90']:
                     actual_start_date = min_date
                     while actual_start_date.weekday() >= 5:
                         actual_start_date += datetime.timedelta(days=1)
                     calc_end_date = None
                else:
                     calc_end_date = prediction['h90']

                DATE_KEYBOARD = date_keyboard(today, 1, days_ahead, start_date=actual_start_date, end_date=calc_end_date, prediction_dist=prediction.get('dist'))
                
                await update.message.reply_text(
                    f"{warn_msg}\n\nЯкщо ви бажаєте залишити цю дату, введіть її ще раз або натисніть кнопку щоб обрати одну з рекомендованих.",
                    parse_mode='Markdown',
                    reply_markup=DATE_KEYBOARD
                )
                return JOIN_GETTING_DATE
            else:
                # Clear warning state if date is good
                context.user_data.pop('warning_shown', None)
                context.user_data.pop('warned_date', None)

    # Створення нового рядка для додавання в DataFrame
    new_entry = {
        'ID': user_id,
        'Дата': chosen_date.strftime("%d.%m.%Y"),
        'Примітки': user_notes,
        'Статус': 'На розгляді', # Додаємо статус "На розгляді"
        'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        'Попередня дата': previous_state, # Використовуємо збережений попередній стан
        **telegram_user_data # Розпаковуємо дані користувача Telegram
    }
    
    new_entry_df = pd.DataFrame([new_entry])
    # Спроба зберегти дані
    if save_queue_data(new_entry_df): # Перевіряємо результат збереження
        # Оновлюємо глобальний DataFrame ТІЛЬКИ ПІСЛЯ УСПІШНОГО ЗБЕРЕЖЕННЯ
        queue_df = pd.concat([queue_df, new_entry_df], ignore_index=True)
        notification_text = f"✅ Користувач {update.effective_user.mention_html()}\nстворив або переніс запис для\nID <code>{user_id}</code> на <code>{chosen_date.strftime('%d.%m.%Y')}</code>" 
        await send_group_notification(context, notification_text)
        message_text = f"Ви успішно створили заявку на запис/перенос дати в черзі!\nВаш ID: `{user_id}`, Обрана дата: `{chosen_date.strftime('%d.%m.%Y')}`\nСтатус заявки: `На розгляді`\nВаша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час з'явиться в жовтій таблиці 🟡TODO."
        await update.message.reply_text(message_text, parse_mode='Markdown', reply_markup=MAIN_KEYBOARD)
        logger.info(f"Запис користувача {get_user_log_info(update.effective_user)} (ID: {user_id}) оновлено/додано на дату: {chosen_date.strftime('%d.%m.%Y')}. Попередня дата: {previous_state if previous_state else 'новий запис'}")
        context.user_data.clear()
        return ConversationHandler.END
    else:
        # Якщо збереження не вдалося
        logger.error(f"Не вдалося зберегти запис користувача {get_user_log_info(update.effective_user)} (ID: {user_id}) на дату: {chosen_date.strftime('%d.%m.%Y')}.")
        await update.message.reply_text(
            "Сталася технічна помилка при збереженні вашого запису. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Завершуємо розмову, щоб користувач міг почати знову
        return ConversationHandler.END

# --- ФУНКЦІЇ ДЛЯ РОЗМОВИ СКАСУВАННЯ ЗАПИСУ (/cancel_record) ---

async def cancel_record_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес скасування запису, просячи користувача ввести ID."""
    if is_banned(update.effective_user.id):
        logger.warning(f"Заблокований користувач {get_user_log_info(update.effective_user)} намагався скасуввати запис.")
        await update.message.reply_text(
            "Ваш обліковвй запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову 
    global queue_df # Оновлюємо DataFrame перед початком діалогу
    queue_df = load_queue_data() # Оновлюємо DataFrame перед початком діалогу

    if queue_df is None: # Перевірка на помилку завантаження
        logger.error(f"Помилка завантаження даних для скасування запису користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав скасування запису.")
    # Зберігаємо дані користувача в context.user_data для подальшого використання
    context.user_data['telegram_user_data'] = get_user_telegram_data(update.effective_user)

    await update.message.reply_text(
        "Будь ласка, введіть номер зі списку первинної черзи для запису, який ви хочете скасувати. "
        "Це може бути ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD # Можна використовувати ForceReply для введення
    )
    return CANCEL_GETTING_ID

async def cancel_record_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID для скасування, перевіряє його та видаляє запис."""
    global queue_df
    
    id_to_cancel = update.message.text.strip()
    telegram_user_data = context.user_data.get('telegram_user_data')

    # Регулярний вираз для перевірки формату ID
    id_pattern = r"^(\d+|\d+\/\d+)$"
    
    if not re.match(id_pattern, id_to_cancel):
        # Клавіатура з кнопкою "Скасувати ввід"
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID для скасування: '{id_to_cancel}'")
        # cancel_keyboard = ReplyKeyboardMarkup([[KeyboardButton(BUTTON_TEXT_CANCEL_OP)]], one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return CANCEL_GETTING_ID # Залишаємося в тому ж стані

    # Знаходимо останній актуальний запис для цього ID
    temp_df_for_prev = queue_df.copy()
    temp_df_for_prev['Змінено_dt'] = pd.to_datetime(temp_df_for_prev['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce').fillna("01.01.2025 00:00:00")

    last_record_for_id = temp_df_for_prev[temp_df_for_prev['ID'] == id_to_cancel].sort_values(by='Змінено_dt', ascending=False)
    # Перевіряємо, чи є актуальний (непорожній) запис.
    # Додаткова умова, що статус не "Скасовано" або "Відхилено", щоб уникнути повторного скасування
    if (not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] != '') or (not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] == '' and last_record_for_id.iloc[0]['Статус'] == 'Відхилено'):
        previous_date = last_record_for_id.iloc[0]['Дата'] # Беремо дату з останнього запису
        # Створюємо новий запис для скасування
        new_entry = {
            'ID': id_to_cancel,
            'Дата': '', # Поле "Дата" робимо порожнім при скасуванні
            'Примітки': '',
            'Статус': 'На розгляді', # Встановлюємо статус
            'Змінено': datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            'Попередня дата': previous_date, # Використовуємо збережений попередній стан
            **telegram_user_data # Розпаковуємо дані користувача Telegram
        }
        
        new_entry_df = pd.DataFrame([new_entry])
        if save_queue_data(new_entry_df): # Перевіряємо результат збереження
            # Оновлюємо глобальний DataFrame ТІЛЬКИ ПІСЛЯ УСПІШНОГО ЗБЕРЕЖЕННЯ
            queue_df = pd.concat([queue_df, new_entry_df], ignore_index=True)
            logger.info(f"Запис з ID '{id_to_cancel}' на `{previous_date}` успішно скасовано користувачем {get_user_log_info(update.effective_user)}.")
            notification_text = f"❎ Користувач {update.effective_user.mention_html()} скасував запис для\nID <code>{id_to_cancel}</code> на <code>{previous_date}</code>" 
            await send_group_notification(context, notification_text)
            await update.message.reply_text(
                f"Ви успішно створили заявку на скасування дати в черзі!\nВаш ID: `{id_to_cancel}` попередній запис на `{previous_date}`\nСтатус заявки: `На розгляді`\nВаша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час зникне з жовтої таблиці 🟡TODO.",
                parse_mode='Markdown',
                reply_markup=MAIN_KEYBOARD
            )
        else:
            logger.error(f"Не вдалося зберегти скасування запису для ID '{id_to_cancel}' користувачем {get_user_log_info(update.effective_user)}.")
            await update.message.reply_text(
                "Cталася помилка при скасуванні вашого запису. Будь ласка, спробуйте повторити спробу пізніше.",
                reply_markup=MAIN_KEYBOARD
            )
    elif not last_record_for_id.empty and last_record_for_id.iloc[0]['Дата'] == '' and last_record_for_id.iloc[0]['Статус'] != 'Відхилено':
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} спробував повторно скасувати запис з ID '{id_to_cancel}'.")
        await update.message.reply_text(
            f"Запит на скасування номеру `{id_to_cancel}` вже прийнято.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD # Додаємо клавіатуру
        )
    else:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} спробував скасувати неіснуючий або вже скасований запис з ID '{id_to_cancel}'.")
        await update.message.reply_text(
            f"Запис з номером `{id_to_cancel}` не знайдено в черзі або він вже скасований.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD # Додаємо клавіатуру
        )
    context.user_data.clear() # Очищуємо тимчасові дані
    return ConversationHandler.END # Завершуємо розмову

# --- ФУНКЦІЇ ДЛЯ РОЗМОВИ ПЕРЕГЛЯДУ СТАТУСУ (BUTTON_TEXT_STATUS) ---

async def status_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес перегляду статусу, просячи користувача ввести ID."""
    global queue_df # Оновлюємо DataFrame перед початком діалогу
    queue_df = load_queue_data()
    if queue_df is None: # Перевірка на помилку завантаження
        logger.error(f"Помилка завантаження даних для перегляду статусу користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав перегляд статусу.")
    await update.message.reply_text(
        "Будь ласка, введіть номер зі списку первинної черги, статус якого ви хочете перевірити. "
        "Це може бути ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
        parse_mode='Markdown',
        reply_markup=CANCEL_KEYBOARD
    )
    return STATUS_GETTING_ID

async def status_get_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує ID від користувача, перевіряє його та відображає статус останнього запису для цього ID."""
    global queue_df
    id_to_check = update.message.text.strip()
    user_tg_id = update.effective_user.id

    # Регулярний вираз для перевірки формату ID
    id_pattern = r"^(\d+|\d+\/\d+)$"

    if not re.match(id_pattern, id_to_check):
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний ID для перевірки статусу: '{id_to_check}'")
        await update.message.reply_text(
            "Невірний формат номеру. Будь ласка, введіть ціле число або два цілих числа, розділені слешем (наприклад, `9999` або `9999/1`).",
            parse_mode='Markdown',
            reply_markup=CANCEL_KEYBOARD
        )
        return STATUS_GETTING_ID # Залишаємося в тому ж стані

    # Знаходимо всі записи, що стосуються цього ID
    id_records = queue_df[queue_df['ID'] == id_to_check].copy() 
    
    if id_records.empty:
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} запитав статус для ID '{id_to_check}'.")
        await update.message.reply_text(
            f"Запис з номером `{id_to_check}` не знайдено.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END

    # Знаходимо останній актуальний запис для цього ID
    id_records['Змінено_dt'] = pd.to_datetime(
        id_records['Змінено'].astype(str),
        format="%d.%m.%Y %H:%M:%S",
        dayfirst=True,
        errors='coerce'
    )
    id_records['Змінено_dt'] = id_records['Змінено_dt'].fillna(datetime.datetime(2025, 1, 1, 0, 0, 0)) # Для старих записів без часу зміни

    latest_record = id_records.sort_values(by='Змінено_dt', ascending=False).iloc[0]

    # Визначаємо, чи є цей запис актуальним (не скасованим)
    is_actual_record = (latest_record['Дата'].strip() != '')

    status_message = f"**Статус запису для номеру:** `{latest_record['ID']}`\n"

    if is_actual_record:
        status_message += f"**Дата запису:** `{latest_record['Дата']}`\n"
        status_message += f"**Поточний статус:** `{latest_record['Статус'] if latest_record['Статус'].strip() else 'Невизначений'}`\n"
        
        # Розрахунок ймовірності
        try:
            stats_df = await get_stats_data()
            if stats_df is not None and not stats_df.empty:
                main_id = extract_main_id(latest_record['ID'])
                prediction = calculate_prediction(main_id, stats_df)
                
                if prediction:
                    record_date = datetime.datetime.strptime(latest_record['Дата'], "%d.%m.%Y").date()
                    dist = prediction['dist']
                    prob = calculate_date_probability(record_date, dist)
                    status_message += f"*Орієнтовна ймовірність зайти в 252 кабінет і розпочати ВЛК:* `{prob:.0f}%`\n"
        except Exception as e:
             logger.error(f"Помилка при розрахунку ймовірності в status_get_id: {e}")

        if latest_record['Попередня дата'].strip():
            status_message += f"**Перенесено з дати:** `{latest_record['Попередня дата']}`\n"
    else:
        status_message += f"**Дата:** `cкасування запису`\n"
        status_message += f"**Поточний статус:** `{latest_record['Статус'] if latest_record['Статус'].strip() else 'Невизначений'}`\n"
        if latest_record['Попередня дата'].strip():
            status_message += f"**Скасовано запис від:** `{latest_record['Попередня дата']}`\n"
    if latest_record['Статус'].strip().lower() == 'ухвалено':
       status_message += f"Вашу заявку ухвалено.\nВона вже або через деякий час з'явиться в жовтій таблиці 🟡TODO."
    elif latest_record['Статус'].strip().lower() == 'на розгляді':
       status_message += f"Ваша заявка на розгляді у адміністраторів.\nЯкщо вона буде \"Ухвалена\", то через деякий час з'явиться в жовтій таблиці 🟡TODO."
    else:
       status_message += f"Примітка:\nСхоже з вашою заявкою виникли проблеми.\nЗверніться до адміністраторів в групі [ВЛК Закревського 81](https://t.me/vlkzakrevskogo81) за роз'ясненнями."
  
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} переглянув статус для ID: {id_to_check}.")
    await update.message.reply_text(status_message, parse_mode='Markdown', reply_markup=MAIN_KEYBOARD)
    context.user_data.clear() # Очищуємо тимчасові дані
    return ConversationHandler.END

# --- ФУНКЦІЇ ДЛЯ РОЗМОВИ ВІДОБРАЖЕННЯ (BUTTON_TEXT_SHOW) ---

async def show_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускає процес відображення черги, пропонуючи вибрати опцію."""
    global queue_df # Оновлюємо DataFrame перед початком діалогу
    queue_df = load_queue_data() 
    if queue_df is None: # Перевірка на помилку завантаження
        logger.error(f"Помилка завантаження даних для перегляду черги користувача {get_user_log_info(update.effective_user)}.")
        await update.message.reply_text(
            "Сталася помилка при завантаженні даних. Будь ласка, спробуйте повторити спробу пізніше.",
            reply_markup=MAIN_KEYBOARD
        )
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END # Завершуємо розмову
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} розпочав перегляд черги.")
    await update.message.reply_text(
        "Як ви хочете переглянути записи?",
        reply_markup=SHOW_OPTION_KEYBOARD
    )
    return SHOW_GETTING_OPTION

async def show_get_option(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує опцію відображення (всі або конкретна дата)."""
    choice = update.message.text.strip()

    if choice == "Показати всі записи":
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} обрав перегляд усіх записів.")
        # Передаємо весь DataFrame до display_queue_data, яка сама відфільтрує актуальні
        await display_queue_data(update, queue_df, title="Усі записи в черзі зі статусом \"Ухвалено\":", reply_markup=MAIN_KEYBOARD)
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END
    elif choice == "Показати записи на конкретну дату":
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} обрав перегляд записів на конкретну дату.")

        today = datetime.date.today()
        DATE_KEYBOARD=date_keyboard(today, 0, days_ahead)

        await update.message.reply_text(
            "Будь ласка, введіть дату, на яку ви хочете переглянути записи, у форматі `ДД.ММ.РРРР`.\n"
            f"Ви можете обрати дату зі списку на {days_ahead} днів або ввести з клавіатури.",
            parse_mode='Markdown',
            reply_markup=DATE_KEYBOARD
        )
        return SHOW_GETTING_DATE
    else:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів невідому опцію перегляду: '{choice}'")
        await update.message.reply_text(
            "Невірна опція. Будь ласка, оберіть `Показати всі записи` або `Показати записи на конкретну дату`, або скасуйте дію.",
            parse_mode='Markdown',
            reply_markup=SHOW_OPTION_KEYBOARD
        )
        return SHOW_GETTING_OPTION

async def show_get_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отримує дату для відображення записів і фільтрує чергу."""
    date_input = update.message.text.strip()
    
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{2,4})', date_input)
    if match:
        date_text = match.group(0)
        try:
            if len(match.group(3)) == 2:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
            else:
                 chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
        except ValueError:
             # Якщо парсинг не вдався
             chosen_date = None
    else:
        # Спроба розпарсити без року (dd.mm) - додаємо поточний рік або наступний
        # ... (логіка якщо треба, але поки залишимо просте)
        date_text = date_input
        chosen_date = None

    try:
        if not chosen_date:
             # Fallback old logic attempt or direct parse
             chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%Y").date()
    except ValueError:
         # Try with 2 digit year as fallback
         try:
            chosen_date = datetime.datetime.strptime(date_text, "%d.%m.%y").date()
         except ValueError:
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати для перегляду: '{date_input}'")
            today = datetime.date.today() # Поточна дата
            DATE_KEYBOARD=date_keyboard(today, 0, days_ahead)
            await update.message.reply_html(
                "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РР</code> (наприклад, 25.12.25) або скасуйте дію.",
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE

    try:
        # chosen_date is already a date object here
        current_date_obj = datetime.date.today()
        # Перевірка, чи дата поточна або пізніша 
        if chosen_date < current_date_obj:
            DATE_KEYBOARD=date_keyboard(current_date_obj, 0, days_ahead)
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів дату ранішу за поточну: '{chosen_date.strftime('%d.%m.%Y')}'")
            await update.message.reply_text(
                f"Дата повинна бути не раніше за поточну (`{current_date_obj.strftime('%d.%m.%Y')}`). Будь ласка, спробуйте ще раз або скасуйте дію.",
                parse_mode='Markdown',
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE
        
        # Перевірка на вихідний день
        if chosen_date.weekday() >= 5:
            today = datetime.date.today() # Поточна дата
            DATE_KEYBOARD=date_keyboard(today, 0, days_ahead)
            logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів  дату що припадає на вихідний: '{chosen_date}'")
            await update.message.reply_text(
                "Ви обрали вихідний день. Записи на вихідні дні не створюються. Будь ласка, оберіть робочий день або скасуйте дію.",
                reply_markup=DATE_KEYBOARD
            )
            return SHOW_GETTING_DATE

        # Отримуємо актуальні записи
        temp_df = queue_df.copy()
        temp_df['Змінено_dt'] = pd.to_datetime(temp_df['Змінено'].astype(str), format="%d.%m.%Y %H:%M:%S", dayfirst=True, errors='coerce')
        temp_df['Змінено_dt'] = temp_df['Змінено_dt'].fillna("01.01.2025 00:00:00")
        #temp_df = temp_df.dropna(subset=['Змінено_dt'])
        actual_records = temp_df.sort_values(by=['ID', 'Змінено_dt'], ascending=[True, True]).drop_duplicates(subset='ID', keep='last')
        actual_queue = actual_records[actual_records['Дата'].astype(str).str.strip() != '']
        # Фільтруємо актуальні записи за обраною датою
        filtered_df = actual_queue[
            (actual_queue['Дата'] == chosen_date.strftime("%d.%m.%Y")) &
            (actual_queue['Статус'].astype(str).str.strip().str.lower() == 'ухвалено')
        ]
        
        logger.info(f"Користувач {get_user_log_info(update.effective_user)} переглянув записи на дату: {chosen_date.strftime('%d.%m.%Y')}")
        next_working_day = current_date_obj + datetime.timedelta(days=1)
        while next_working_day.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            next_working_day += datetime.timedelta(days=1)
        if chosen_date == next_working_day:
            await display_queue_data(update, filtered_df, title=f"Поточна черга зі статусом \"Ухвалено\" на `{chosen_date.strftime('%d.%m.%Y')}`:\n", reply_markup=MAIN_KEYBOARD, iConfirmation = False) #iConfirmation статус про підтвердження візиту на завтра при перегляді черги на завтра
        else:
            await display_queue_data(update, filtered_df, title=f"Поточна черга зі статусом \"Ухвалено\" на `{chosen_date.strftime('%d.%m.%Y')}`:\n", reply_markup=MAIN_KEYBOARD)
        context.user_data.clear() # Очищуємо тимчасові дані
        return ConversationHandler.END

    except ValueError:
        logger.warning(f"Користувач {get_user_log_info(update.effective_user)} ввів некоректний формат дати для перегляду: '{date_input}'")
        today = datetime.date.today() # Поточна дата
        DATE_KEYBOARD=date_keyboard(today, 0, days_ahead)
        await update.message.reply_html(
            "Невірний формат дати. Будь ласка, введіть дату у форматі <code>ДД.ММ.РРРР</code> (наприклад, 25.12.2025) або скасуйте дію.",
            reply_markup=DATE_KEYBOARD
        )
        return SHOW_GETTING_DATE

# --- ПОВІДОМЛЕННЯ ОНОВЛЕННЯ СТАТУСІВ ЗА РОЗКЛАДОМ ---

async def notify_status(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Функція для відстеження зміни статусу запису та надсилання сповіщень.
    Призначена для запуску за розкладом.
    """
    logger.info("Початок перевірки зміни статусів записів.")
    
    # 1. Завантажуємо дані з Google Sheets (використовуйте вашу функцію)
    global queue_df
    queue_df = load_queue_data()
    
    # 2. Очищаємо та готуємо дані
    queue_df['Змінено_dt'] = pd.to_datetime(queue_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    queue_df['Змінено_dt'] = queue_df['Змінено_dt'].fillna("01.01.2025 00:00:00")
    #queue_df.dropna(subset=['Дата', 'Примітки', 'Статус', 'Змінено'], inplace=True)
    queue_df.dropna(inplace=True)
    queue_df['TG ID'] = queue_df['TG ID'].astype(str)    

    # 3. Знаходимо найактуальніший запис для кожного користувача
    latest_entries = queue_df.loc[queue_df.groupby('ID')['Змінено_dt'].idxmax()]

    # 4. Завантажуємо останній відомий стан
    last_known_state = load_status_state()
    
    # 5. Перевіряємо зміни та відправляємо сповіщення
    new_state = {}
    for index, row in latest_entries.iterrows():
        user_id = row['ID']
        target_date = row['Дата']
        note = row['Примітки']
        current_status = row['Статус']
        modified = row['Змінено']
        prev_date = row['Попередня дата']
        tg_id = row['TG ID']
              
        last_status_info = last_known_state.get(user_id)

        if not last_status_info:
            confirmation = ''
        elif 'confirmation' not in last_status_info:
            confirmation = ''
        else:
            confirmation = last_status_info['confirmation']
      
        # Якщо стан змінився або це новий запис
        if ((not last_status_info) 
            or (last_status_info['status'] != current_status and last_status_info['date'] == target_date and last_status_info['modified'] == modified)
            or (last_status_info['date'] != target_date or last_status_info['modified'] != modified)
        ):
            # Формуємо текст повідомлення
            if current_status != 'На розгляді':
                if target_date != '':
                    to_date = f" на <code>{target_date}</code>"
                    if prev_date != '':
                        rmc = 'перенесення' 
                    else:
                           rmc = 'створення'
                else:
                    rmc = 'скасування'
                    to_date = ""
                emo = '🟢' if current_status == 'Ухвалено' else '🔴'
                notification_text = f"{emo} Заявку на {rmc} запису ID <code>{user_id}</code> {to_date}\n<code>{current_status}</code>"
                notification_warning = f'\nПримітка: <code>{note}</code>' if note !='' else ''
                notification = notification_text+notification_warning
                # Надсилаємо сповіщення в групу
                #await send_group_notification(context, notification)
                # Надсилаємо особисте повідомлення користувачу
                await send_user_notification(context, tg_id, notification)
        # Оновлюємо стан для збереження
        new_state[user_id] = {
            'date': target_date,
            'status': current_status,
            'modified': modified,
            'confirmation': confirmation
        }

    # 6. Зберігаємо оновлений стан
    save_status_state(new_state)
    logger.info("Завершення перевірки зміни статусів записів.")
    
# --- ПОВІДОМЛЕННЯ НАГАДУВАННЯ ПРО ЗАПИС ЗА РОЗКЛАДОМ ---
async def date_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Функція для нагадування запланованого візиту.
    Призначена для запуску за розкладом.
    """
    logger.info("Початок процедури нагадування і підтвердження дати візиту.")
    
    # 1. Завантажуємо дані з Google Sheets (використовуйте вашу функцію)
    global queue_df
    queue_df = load_queue_data()
    
    # 2. Очищаємо та готуємо дані
    queue_df['Змінено_dt'] = pd.to_datetime(queue_df['Змінено'], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    queue_df['Змінено_dt'] = queue_df['Змінено_dt'].fillna("01.01.2025 00:00:00")
    queue_df['Дата_dt'] = pd.to_datetime(queue_df['Дата'], format="%d.%m.%Y", errors='coerce').dt.date
    queue_df.dropna(inplace=True)
    queue_df['TG ID'] = queue_df['TG ID'].astype(str)    

    # 3. Знаходимо найактуальніший запис для кожного користувача
    latest_entries = queue_df.loc[queue_df.groupby('ID')['Змінено_dt'].idxmax()]
    
    # 4. Знаходимо дати на сьогодні, через день і три дні
    current_date_obj = datetime.date.today()
    # Define a timedelta of 1 day
    one_day_later = current_date_obj + datetime.timedelta(days=1)
    # Define a timedelta of 3 days
    three_days_later = current_date_obj + datetime.timedelta(days=3)
    
    # 5. Перевіряємо дати та відправляємо сповіщення
    for index, row in latest_entries.iterrows():
        user_id = row['ID']
        target_date = row['Дата']
        target_date_dt = row['Дата_dt']
        note = row['Примітки']
        current_status = row['Статус']
        modified = row['Змінено']
        prev_date = row['Попередня дата']
        tg_id = row['TG ID']
        remind = False
        poll_confirm = False
     
        # Перевіряємо дати запису
        if target_date_dt == current_date_obj:
            remind = True
            nr_days = 'на сьогодні'        
        if target_date_dt == one_day_later:
            poll_confirm = False # тимчасово відключаємо підтвердження візиту (голосування) поки не зробимо повний список на завтра
            remind = True
            nr_days = 'на завтра'
        if target_date_dt == three_days_later:
            remind = True
            nr_days = 'за 3 дні'
        
        if remind and current_status == 'Ухвалено':
            # Формуємо текст повідомлення
            emo = '❗️'
            notification_text = f"{emo}<code>Нагадування!</code>\n  Для вашого номеру <code>{user_id}</code> призначено візит {nr_days}: <code>{target_date}</code>"
            notification_warning = f'\nПримітка: <code>{note}</code>' if note !='' else ''
            notification = notification_text+notification_warning
            # Надсилаємо сповіщення в групу
            #await send_group_notification(context, notification)
            # Надсилаємо особисте повідомлення користувачу
            await send_user_notification(context, tg_id, notification)
 
        if poll_confirm and current_status == 'Ухвалено' and tg_id != '':
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Так", callback_data=f"{CALLBACK_YES}_{user_id}")],
                [InlineKeyboardButton("❌ Ні", callback_data=f"{CALLBACK_NO}_{user_id}")]
            ])
            try:
                message = await context.bot.send_message(
                    chat_id=tg_id,
                    text=f"<b>Підтвердження візиту.\nВаш номер в списку первинної черги:</b> <code>{user_id}</code>\n\n"
                         "Ви підтверджуєте свій візит на завтра?",
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                # Зберігаємо ID повідомлення для подальшого видалення
                context.job_queue.run_once(
                    callback=delete_confirmation_message,
                    when=datetime.time(hour=23, minute=45),
                    data={'chat_id': tg_id, 'message_id': message.message_id}
                )
                logger.info(f"Опитування відправлено користувачу ID: {tg_id}")
            except Exception as e:
                logger.error(f"Не вдалося відправити опитування користувачу ID {tg_id}: {e}")

    logger.info("Завершення процедури нагадування і підтвердження дати візиту.")
    
async def delete_confirmation_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Видаляє повідомлення-опитування, якщо користувач не відреагував."""
    job_data = context.job.data
    try:
        await context.bot.delete_message(
            chat_id=job_data['chat_id'],
            message_id=job_data['message_id']
        )
        logger.info(f"Повідомлення опитування видалено для {job_data['chat_id']}.")
    except Exception as e:
        logger.error(f"Помилка при видаленні повідомлення опитування: {e}")

async def handle_poll_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user_tg_id = str(query.from_user.id)
    callback_data = query.data
    match = re.match(r'(confirm_yes|confirm_no)_(.+)', callback_data)
    response, user_id = match.groups()
    #response, user_id = callback_data.split("_", 1)
    
    # Видаляємо опитування після відповіді
    try:
        await query.message.delete()
    except Exception as e:
        logger.error(f"Не вдалося видалити опитування після відповіді: {e}")
       
    # Визначаємо, яке повідомлення надіслати користувачу
    user_reply_text = ""
    notification_text = ""
    confirmation = ""

    if response == CALLBACK_YES:
        user_reply_text = "Дякуємо, ваш візит підтверджено!"
        notification_text = f"✅ Користувач ID <code>{user_id}</code> підтвердив свій візит на завтра."
        confirmation = "Підтвердив візит"
    elif response == CALLBACK_NO:
        user_reply_text = "Ваша відповідь прийнята. Зверніть увагу, що Ваш запис не скасовано. Будь ласка перенесіть або скасуйте запис, не створюйте незручностей тим хто за Вами."
        notification_text = f"❌ Користувач ID <code>{user_id}</code> відмовився від візиту на завтра\n(запис не скасовано)."
        confirmation = "Відмовився від візиту"

    # Завантажуємо останній відомий стан
    last_known_state = load_status_state()
    if user_id in last_known_state:
        last_known_state[user_id]['confirmation'] = confirmation
        save_status_state(last_known_state)
    else:
        logger.warning(f"Користувача {user_id} не знайдено в записах.")
    
    # Надсилаємо сповіщення в груповий чат
    #if notification_text:
        #await send_group_notification(context, notification_text)

    # Надсилаємо особисте повідомлення користувачу з основною клавіатурою
    if user_reply_text:
        await send_user_notification(context, user_tg_id, user_reply_text)
        
# --- ЗАГАЛЬНІ ФУНКЦІЇ ДЛЯ РОЗМОВИ ---

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Скасовує поточну розмову."""
    logger.info(f"Користувач {get_user_log_info(update.effective_user)} скасував поточну операцію.")
    await update.message.reply_text(
        "Дію скасовано. Оберіть наступну команду:",
        reply_markup=MAIN_KEYBOARD # Додаємо клавіатуру
    )
    context.user_data.clear()
    return ConversationHandler.END

async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.type == 'private':
        """Обробляє повідомлення, які не відповідають очікуванням в рамках розмови."""
        logger.warning(f"Невідоме повідомлення від користувача {get_user_log_info(update.effective_user)}: '{update.message.text}'")
        await update.message.reply_text(
            "Будь ласка, дотримуйтесь інструкцій або скористайтеся кнопкою `Скасувати ввід`.",
            parse_mode='Markdown',
            reply_markup=MAIN_KEYBOARD # Додаємо клавіатуру
        )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обробляє помилки, що виникли в боті.""" 
    # context.error містить оригінальну помилку
    error_message = str(context.error)   
    # Перевіряємо, чи це мережева помилка
    if isinstance(context.error, ConnectError):
        logger.critical(f"Помилка з'єднання. Бот не може підключитися до Telegram API. Помилка: {error_message}")
        # Тут можна додати логіку для вимкнення бота, якщо це необхідно
        return

    # Якщо це інша помилка, обробляємо її як зазвичай
    logger.error("Виникла непередбачена помилка: %s", error_message)  
    # Логуємо повний traceback для детального аналізу
    # logger.error("Повний traceback:", exc_info=context.error)
    
    if update is None:
        logger.warning("Помилка не пов'язана з оновленням. Ймовірно, проблема зі зв'язком.")
        # Нічого не робимо, фреймворк сам відновить з'єднання
        return

    # Якщо об'єкт 'update' існує, можна повідомити користувача про помилку
    if update and update.effective_message:
        if update.message.chat.type == 'private':
            try:
                await update.effective_message.reply_text(
                    "Вибачте, виникла внутрішня помилка. Будь ласка, виконайте спробу пізніше."
                )
            except Exception as e:
                logger.error("Не вдалося відправити повідомлення про помилку користувачу: %s", e)

def main() -> None:
    initialize_bot()
    application = (
        Application.builder()
        .token(TOKEN)
        .http_version("1.1") # Зазвичай допомагає зі стабільністю
        .read_timeout(30.0)  # Таймаут на читання відповіді
        .write_timeout(30.0) # Таймаут на запис запиту
        .connect_timeout(30.0) # Таймаут на встановлення з'єднання
        .pool_timeout(30.0)  # Таймаут пулу з'єднань
        .build()
   )
    # Register the error handler
    application.add_error_handler(error_handler)
    # Обробник для /start завжди CommandHandler
    application.add_handler(CommandHandler("start", start))
    # Обробники команд для керування адміністраторами
    application.add_handler(CommandHandler("grant_admin", grant_admin))
    application.add_handler(CommandHandler("drop_admin", drop_admin))
    # Обробники команд для керування списком заблокованих
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))

    # --- ConversationHandlers повинні бути додані ПЕРШИМИ ---
    # Це дає їм пріоритет над іншими MessageHandler, коли розмова активна.

    # Обробник для кнопки "Скасувати ввід"
    cancel_button_handler = MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_CANCEL_OP), cancel_conversation)

    join_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_JOIN), join_start)],
        states={
            JOIN_GETTING_ID: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, join_get_id)
            ],
            JOIN_GETTING_DATE: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, join_get_date)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation) # Залишаємо на випадок ручного вводу /cancel
        ],
        conversation_timeout=3600,  # Timeout in seconds (e.g., 3600 seconds)
        allow_reentry=True
    )

    cancel_record_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_CANCEL_RECORD), cancel_record_start)],
        states={
            CANCEL_GETTING_ID: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, cancel_record_get_id)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation)
        ],
        conversation_timeout=3600,  # Timeout in seconds (e.g., 3600 seconds)
        allow_reentry=True
    )

    show_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_SHOW), show_start)],
        states={
            SHOW_GETTING_OPTION: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, show_get_option)
            ],
            SHOW_GETTING_DATE: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, show_get_date)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation)
        ],
        conversation_timeout=3600,  # Timeout in seconds (e.g., 3600 seconds)
        allow_reentry=True
    )

    status_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_STATUS), status_start)],
        states={
            STATUS_GETTING_ID: [
                cancel_button_handler, # Переміщуємо на початок списку
                MessageHandler(filters.TEXT & ~filters.COMMAND, status_get_id)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation)
        ],
        conversation_timeout=3600,  # Timeout in seconds (e.g., 3600 seconds)
        allow_reentry=True
    )

    application.add_handler(join_conv_handler)
    application.add_handler(cancel_record_conv_handler)
    application.add_handler(show_conv_handler)
    application.add_handler(status_conv_handler)
    application.add_handler(CallbackQueryHandler(handle_poll_response, pattern="^confirm_"))
    
    # --- Загальні обробники для окремих кнопок (НЕ розмов) ---
    # Вони мають бути після ConversationHandler, але до загального fallback
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_OPEN_SHEET), open_sheet_command))
    #application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_CLEAR_QUEUE), clear_queue_command))
    # Обробник кнопки "Скасувати ввід" поза розмовами.
    # Він вже доданий як fallback у кожному ConversationHandler,
    # і також як окремий обробник тут, щоб спрацьовувати, якщо користувач просто натисне її,
    # коли немає активної розмови, і таким чином повернути MAIN_KEYBOARD.
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(BUTTON_TEXT_CANCEL_OP), cancel_conversation)) # Обробник кнопки "Скасувати ввід" поза розмовами

    # --- Загальний fallback обробник ---
    # Цей обробник має бути ДОДАНИЙ ОСТАННІМ!
    # Він ловить ВСЕ, що не було оброблено попередніми обробниками.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback))

    # --- Налаштування планувальника ---
    kyiv_tz = timezone('Europe/Kyiv')        
    
    # Це завдання буде запускатися щоденно о 3:00
    application.job_queue.run_daily(
        callback=perform_queue_cleanup,
        time=datetime.time(hour=3, minute=0, tzinfo=kyiv_tz),
        name="Daily Queue Cleanup"
    )
    logger.info(f"Завдання 'Daily Queue Cleanup' заплановано щоденно о 03:00 за {kyiv_tz.tzname(datetime.datetime.now())}")

    # Це завдання буде запускатися щоденно о 7:10
    application.job_queue.run_daily(
        callback=date_reminder,
        time=datetime.time(hour=7, minute=10, tzinfo=kyiv_tz),
        job_kwargs={'misfire_grace_time': 30 * 60}, # Дозволяє пропустити запуск, якщо бот був офлайн
        name="Visit Reminder"
    )
    logger.info(f"Завдання 'Visit Reminder' заплановано щоденно о 07:10 за {kyiv_tz.tzname(datetime.datetime.now())}")
    
    # Запускаємо кожні 30 хвилин. Сама функція перевірить, чи час підходить.
    application.job_queue.run_repeating(
        callback=notify_status,
        interval=datetime.timedelta(minutes=5),
        first=datetime.time(hour=7, minute=3, tzinfo=kyiv_tz), # Перший запуск о 7:00
        last=datetime.time(hour=23, minute=33, tzinfo=kyiv_tz), # Останній запуск о 23:30
        name="Status Change Notification"
    )
    logger.info(f"Завдання 'Status Change Notification' заплановано кожні 30 хвилин з 07:00 по 23:30 за {kyiv_tz.tzname(datetime.datetime.now())}")
    
    # --- Запуск бота з обробкою зупинки ---
    logger.info("Присвячується добровольцям і волонтерам.")
    logger.info("Бот запису в електронну чергу на ВЛК Закревського,81/1 запущено...") # Переміщено сюди
    logger.info("Планувальник APScheduler запущено.")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except ConnectError as e:
        logger.error(f"Помилка з'єднання: відсутнє підключення до Telegram API. Код помилки: {e}")
    except KeyboardInterrupt:
        logger.info("Бот отримав сигнал KeyboardInterrupt. Завершення роботи...")
    finally:
        logger.info("Бот зупинено.")

if __name__ == "__main__":
    main()
