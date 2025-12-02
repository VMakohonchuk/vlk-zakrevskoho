import pytest
import pandas as pd
import datetime
import numpy as np
import re
from unittest.mock import MagicMock, patch, AsyncMock
from telegram import Update, User, Message, Chat
from telegram.ext import ContextTypes

# Імпорт модуля, що тестується
# Припускаємо, що модуль знаходиться в тій же директорії або в PYTHONPATH
import VLK_Zakrevskoho_81_BOT as bot

# Ініціалізація глобальних змінних для тестування (хоча ми переважно використовуємо моки)
bot.ADMIN_IDS = [12345]
bot.BANLIST = []

# --- Фікстури ---
@pytest.fixture
def mock_update():
    update = MagicMock(spec=Update)
    update.effective_user = MagicMock(spec=User)
    update.effective_user.id = 12345
    update.effective_user.username = "testuser"
    update.effective_user.full_name = "Test User"
    update.effective_user.mention_html.return_value = "<a href='tg://user?id=12345'>Test User</a>"
    
    update.message = MagicMock(spec=Message)
    update.message.text = "some text"
    update.message.chat = MagicMock(spec=Chat)
    update.message.chat.type = 'private'
    # Використовуємо AsyncMock для асинхронних методів
    update.message.reply_text = AsyncMock()
    update.message.reply_html = AsyncMock()
    update.message.reply_photo = AsyncMock()
    return update

@pytest.fixture
def mock_context():
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.user_data = {}
    context.bot = MagicMock()
    context.bot.send_message = AsyncMock()
    context.args = []
    return context

@pytest.fixture
def sample_queue_df():
    return pd.DataFrame({
        'ID': ['100', '101'],
        'Дата': ['01.01.2025', '02.01.2025'],
        'Примітки': ['', 'note'],
        'Статус': ['Ухвалено', 'На розгляді'],
        'Змінено': ['01.01.2025 10:00:00', '01.01.2025 11:00:00'],
        'Попередня дата': ['', ''],
        'TG ID': [111, 222],
        'TG Name': ['user1', 'user2'],
        'TG Full Name': ['User One', 'User Two']
    }, columns=bot.REQUIRED_COLUMNS)

# --- Тести допоміжних функцій ---

def test_get_ordinal_date():
    # 1970-01-05 - це понеділок (якірна точка) -> 0
    assert bot.get_ordinal_date(datetime.date(1970, 1, 5)) == 0
    # 1970-01-09 - це п'ятниця (+4 дні) -> 4
    assert bot.get_ordinal_date(datetime.date(1970, 1, 9)) == 4
    # 1970-01-10 - це субота (+5 днів) -> 5 (обмежено 5 для тижня)
    assert bot.get_ordinal_date(datetime.date(1970, 1, 10)) == 5
    # 1970-01-12 - це понеділок (+7 днів) -> 5 (1 тиждень * 5) + 0 = 5
    assert bot.get_ordinal_date(datetime.date(1970, 1, 12)) == 5

def test_get_date_from_ordinal():
    anchor = datetime.date(1970, 1, 5)
    assert bot.get_date_from_ordinal(0) == anchor
    assert bot.get_date_from_ordinal(5) == datetime.date(1970, 1, 12)

def test_extract_main_id():
    assert bot.extract_main_id("123") == 123
    assert bot.extract_main_id("123/1") == 123
    assert bot.extract_main_id("abc") is None
    assert bot.extract_main_id(123) is None # Логіка очікує рядок, але функція обробляє безпечно?
    # Функція: if isinstance(id_string, str): ...
    assert bot.extract_main_id(123) is None

def test_is_admin():
    bot.ADMIN_IDS = [123, 456]
    assert bot.is_admin(123) is True
    assert bot.is_admin(999) is False

def test_is_banned():
    bot.BANLIST = [111]
    assert bot.is_banned(111) is True
    assert bot.is_banned(222) is False

def test_calculate_end_date():
    # Початок у понеділок, додаємо 1 день -> вівторок
    start = datetime.date(2023, 1, 2) 
    # Логіка функції: 
    # якщо день тижня початку < 5 (Пн-Пт): added = 1
    # поки added < days_count: ...
    
    # Випадок 1: days_count = 1.
    # початок Пн. added=1. Цикл не запускається. Повертає Пн. 
    # Чекайте, логіка каже: "Розрахувати дату завершення, додаючи вказану кількість робочих днів".
    # Якщо я прошу на 1 день наперед, і сьогодні понеділок, я маю на увазі сьогодні чи завтра?
    # Якщо days_ahead=15 (за замовчуванням), зазвичай означає відображення кнопок на 15 днів.
    
    # Давайте простежимо calculate_end_date(Пн, 2)
    # added = 1.
    # цикл: added(1) < 2.
    #   temp_date += 1 (Вт). Вт < 5 -> added=2.
    # цикл завершується. Повертає Вт.
    
    assert bot.calculate_end_date(start, 2) == datetime.date(2023, 1, 3)

def test_date_keyboard_format():
    # Перевірка формату кнопок (dd.mm.yy)
    today = datetime.date(2025, 12, 1)
    keyboard = bot.date_keyboard(today=today, days_to_check=1, days_ahead=1)
    
    # Очікуємо, що перша кнопка буде для 02.12.25 (наступний день)
    # Клавіатура повертає об'єкт ReplyKeyboardMarkup, який має атрибут keyboard (список списків)
    buttons = keyboard.keyboard
    # Знаходимо першу кнопку з датою
    # buttons structure: [[Button1, Button2...], [Button...]]
    first_button_text = buttons[0][0].text
    
    # Перевіряємо наявність року у тексті (2 цифри)
    assert "02.12.25" in first_button_text
    # Перевіряємо, що це 2 цифри року
    assert re.search(r'\d{2}\.\d{2}\.\d{2}', first_button_text)

# --- Тести логіки прогнозування ---

def test_calculate_prediction_insufficient_data():
    assert bot.calculate_prediction(100, None) is None
    assert bot.calculate_prediction(100, pd.DataFrame()) is None
    
    # Dataframe з кількома рядками
    df = pd.DataFrame({
        'Останній номер що зайшов': ['1', '2'],
        'Дата прийому': ['01.01.2025', '02.01.2025']
    })
    assert bot.calculate_prediction(100, df) is None

def test_calculate_prediction_valid():
    # Створити синтетичний лінійний тренд
    # Дати: Пн, Вт, Ср, Чт, Пт (5 днів)
    # ID: 10, 20, 30, 40, 50
    dates = [
        '01.01.2024', # Mon
        '02.01.2024', # Tue
        '03.01.2024', # Wed
        '04.01.2024', # Thu
        '05.01.2024', # Fri
        '08.01.2024'  # Mon
    ]
    ids = [10, 20, 30, 40, 50, 60]
    
    df = pd.DataFrame({
        'Останній номер що зайшов': ids,
        'Дата прийому': dates
    })
    
    # Прогноз для ID 70.
    res = bot.calculate_prediction(70, df)
    
    assert res is not None
    assert isinstance(res['mean'], datetime.date)
    assert isinstance(res['l90'], datetime.date)
    assert isinstance(res['h90'], datetime.date)

# --- Тести обробників команд (асинхронні) ---

@pytest.mark.asyncio
async def test_start_private_chat(mock_update, mock_context):
    mock_update.message.chat.type = 'private'
    await bot.start(mock_update, mock_context)
    
    # Перевірити відповідь
    assert mock_update.message.reply_photo.called or mock_update.message.reply_html.called
    
@pytest.mark.asyncio
async def test_join_start_banned(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.is_banned', return_value=True):
        res = await bot.join_start(mock_update, mock_context)
        assert res == -1 # ConversationHandler.END
        mock_update.message.reply_text.assert_called_with(
            "Ваш обліковвй запис заблоковано. Зверніться до адміністраторів щоб розблокувати.",
            reply_markup=bot.MAIN_KEYBOARD
        )

@pytest.mark.asyncio
async def test_join_start_success(mock_update, mock_context):
    with patch('VLK_Zakrevskoho_81_BOT.is_banned', return_value=False):
        with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=pd.DataFrame(columns=bot.REQUIRED_COLUMNS)):
            res = await bot.join_start(mock_update, mock_context)
            assert res == bot.JOIN_GETTING_ID
            assert mock_context.user_data['telegram_user_data']['TG ID'] == 12345

@pytest.mark.asyncio
async def test_join_get_id_invalid(mock_update, mock_context):
    mock_update.message.text = "invalid_id"
    res = await bot.join_get_id(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_ID
    mock_update.message.reply_text.assert_called()
    assert "Невірний формат" in mock_update.message.reply_text.call_args[0][0]

@pytest.mark.asyncio
async def test_join_get_id_valid_no_stats(mock_update, mock_context):
    mock_update.message.text = "999"
    bot.queue_df = pd.DataFrame(columns=bot.REQUIRED_COLUMNS)
    
    with patch('VLK_Zakrevskoho_81_BOT.check_id_for_queue', new_callable=AsyncMock) as mock_check:
        mock_check.return_value = (True, "")
        with patch('VLK_Zakrevskoho_81_BOT.get_stats_data', new_callable=AsyncMock) as mock_stats:
            mock_stats.return_value = None
            
            res = await bot.join_get_id(mock_update, mock_context)
            
            assert res == bot.JOIN_GETTING_DATE
            assert mock_context.user_data['temp_id'] == "999"
            # Має показати клавіатуру з датами
            assert "Виберіть бажану дату запису" in mock_update.message.reply_text.call_args[0][0]

@pytest.mark.asyncio
async def test_join_get_date_invalid(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    mock_update.message.text = "invalid date"
    
    res = await bot.join_get_date(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_DATE
    assert "Невірний формат дати" in mock_update.message.reply_html.call_args[0][0]

@pytest.mark.asyncio
async def test_join_get_date_past(mock_update, mock_context):
    mock_context.user_data = {'temp_id': '999'}
    past_date = datetime.date.today() - datetime.timedelta(days=1)
    mock_update.message.text = past_date.strftime("%d.%m.%Y")
    
    res = await bot.join_get_date(mock_update, mock_context)
    assert res == bot.JOIN_GETTING_DATE
    assert "Дата повинна бути пізнішою за поточну" in mock_update.message.reply_text.call_args[0][0]

@pytest.mark.asyncio
async def test_join_get_date_success(mock_update, mock_context):
    mock_context.user_data = {
        'temp_id': '999', 
        'telegram_user_data': {'TG ID': 12345, 'TG Name': 'test', 'TG Full Name': 'Test'},
        'previous_state': '',
        'user_notes': ''
    }
    
    # Забезпечити майбутній робочий день
    future_date = datetime.date.today() + datetime.timedelta(days=10)
    while future_date.weekday() >= 5:
        future_date += datetime.timedelta(days=1)
        
    mock_update.message.text = future_date.strftime("%d.%m.%Y")
    bot.queue_df = pd.DataFrame(columns=bot.REQUIRED_COLUMNS)
    
    with patch('VLK_Zakrevskoho_81_BOT.save_queue_data', return_value=True):
        res = await bot.join_get_date(mock_update, mock_context)
        assert res == -1 # END
        assert "успішно створили заявку" in mock_update.message.reply_text.call_args[0][0]
        # Перевірити, чи оновлено queue_df
        assert not bot.queue_df.empty
        assert bot.queue_df.iloc[-1]['ID'] == '999'

# --- Команди адміністратора ---

@pytest.mark.asyncio
async def test_grant_admin_unauthorized(mock_update, mock_context):
    bot.ADMIN_IDS = [999] # Запитувач (12345) не є адміністратором
    await bot.grant_admin(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with("У вас недостатньо прав для виконання цієї команди.", reply_markup=bot.MAIN_KEYBOARD)

@pytest.mark.asyncio
async def test_grant_admin_success(mock_update, mock_context):
    bot.ADMIN_IDS = [12345]
    # Налаштування моку конфігурації
    bot.config['BOT_SETTINGS'] = {'ADMIN_IDS': '12345'}
    
    mock_context.args = ["67890"]
    
    with patch('VLK_Zakrevskoho_81_BOT.save_config'):
        await bot.grant_admin(mock_update, mock_context)
        
    assert 67890 in bot.ADMIN_IDS
    assert "успішно доданий" in mock_update.message.reply_text.call_args[0][0]

# --- Логіка очищення ---

@pytest.mark.asyncio
async def test_perform_queue_cleanup(sample_queue_df):
    # Налаштування дат
    today = datetime.date.today()
    past = today - datetime.timedelta(days=10)
    future = today + datetime.timedelta(days=10)
    
    df = pd.DataFrame({
        'ID': ['1', '2', '3'],
        'Дата': [past.strftime("%d.%m.%Y"), future.strftime("%d.%m.%Y"), ''],
        'Статус': ['Ухвалено', 'Ухвалено', 'Відхилено'],
        'Змінено': [
            '01.01.2023 10:00:00', # Старий
            '01.01.2025 10:00:00', # Майбутнє
            '01.01.2023 10:00:00'  # Старий відхилений з порожньою датою
        ],
        'TG ID': ['1', '2', '3']
    }, columns=bot.REQUIRED_COLUMNS) # Забезпечити наявність інших колонок
    
    # Заповнити відсутні колонки
    for col in bot.REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ''
            
    with patch('VLK_Zakrevskoho_81_BOT.load_queue_data', return_value=df):
        with patch('VLK_Zakrevskoho_81_BOT.save_queue_data_full', return_value=True):
            # Логіка очищення:
            # Видаляє записи з датою < сьогодні
            # Видаляє відхилені записи, старіші за max_mod_idx для цього ID?
            
            removed = await bot.perform_queue_cleanup()
            
            # ID 1 у минулому -> видалено
            # ID 2 у майбутньому -> залишено
            # ID 3 порожня дата, відхилено, старий -> ймовірно видалено, якщо логіка обробляє "відхилені з порожньою датою" як сміття або залишає?
            # Логіка:
            # index_to_drop.extend(sort_df.loc[(sort_df['Дата_dt'].dt.date < current_date_obj) ...
            # Отже, ID 1 має бути видалено.
            
            assert removed >= 1
            assert len(bot.queue_df) < 3
            assert '2' in bot.queue_df['ID'].values # Майбутнє має бути там
