"""
Модуль для синхронізації та аналізу щоденних аркушів.
"""

import os
import datetime
import csv
import pandas as pd
import logging
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

DAILY_SHEETS_CACHE_DIR = "daily_sheets_cache"

def ensure_cache_dir():
    """Створює директорію для кешу якщо не існує."""
    os.makedirs(DAILY_SHEETS_CACHE_DIR, exist_ok=True)

def download_stats(sheets_service, stats_sheet_id, stats_worksheet_name):
    """Завантажує stats аркуш."""
    try:
        range_name = f"{stats_worksheet_name}!A:Z"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=stats_sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return None
        
        headers = values[0]
        data = values[1:]
        
        max_cols = len(headers)
        normalized_data = []
        for row in data:
            if len(row) < max_cols:
                normalized_data.append(row + [''] * (max_cols - len(row)))
            else:
                normalized_data.append(row[:max_cols])
        
        df = pd.DataFrame(normalized_data, columns=headers)
        
        stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
        df.to_csv(stats_file, index=False)
        logger.info(f"Stats оновлено: {len(df)} рядків")
        
        return df
        
    except Exception as e:
        logger.error(f"Помилка завантаження stats: {e}")
        return None

def download_daily_sheet(sheets_service, stats_sheet_id, sheet_name):
    """Завантажує один щоденний аркуш за назвою."""
    try:
        date_obj = datetime.datetime.strptime(sheet_name, "%d.%m.%Y").date()
        cache_filename = date_obj.strftime("%Y-%m-%d.csv")
    except ValueError:
        logger.error(f"Невірний формат дати: {sheet_name}")
        return False
    
    cache_file = os.path.join(DAILY_SHEETS_CACHE_DIR, cache_filename)
    
    try:
        range_name = f"{sheet_name}!A:Z"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=stats_sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logger.warning(f"Аркуш {sheet_name} порожній")
            return False
        
        with open(cache_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in values:
                writer.writerow(row)
        
        logger.debug(f"Завантажено {sheet_name} -> {cache_filename}")
        return True
        
    except HttpError as err:
        if err.resp.status != 400:
            logger.error(f"HTTP помилка для {sheet_name}: {err.resp.status}")
        return False
    except Exception as e:
        logger.error(f"Помилка завантаження {sheet_name}: {e}")
        return False

def sync_daily_sheets(sheets_service, stats_sheet_id, stats_worksheet_name, force_refresh_stats=False, force_refresh_all_sheets=False):
    """
    Синхронізує щоденні аркуші на основі колонки "Аркуш" зі stats.
    Оновлює stats якщо він застарів або force_refresh_stats=True.
    
    ВАЖЛИВО: Завжди перезавантажує останні 5 днів, щоб захопити оновлення
    які адміністратори могли внести протягом дня після попередньої синхронізації.
    
    Args:
        sheets_service: Google Sheets API service
        stats_sheet_id: ID таблиці Stats
        stats_worksheet_name: Назва worksheet зі статистикою
        force_refresh_stats: Примусово оновити stats (ігнорувати кеш)
        force_refresh_all_sheets: Примусово перезавантажити ВСІ щоденні аркуші
    """
    ensure_cache_dir()
    
    stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
    should_refresh = force_refresh_stats
    
    # Перевіряємо чи stats актуальний
    if not should_refresh and os.path.exists(stats_file):
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(stats_file))
        if mod_time.date() < datetime.date.today():
            should_refresh = True
            logger.info("Stats застарів, оновлюємо...")
    else:
        should_refresh = True
    
    if should_refresh:
        stats_df = download_stats(sheets_service, stats_sheet_id, stats_worksheet_name)
        if stats_df is None:
            logger.error("Не вдалося завантажити stats")
            return False
    else:
        stats_df = pd.read_csv(stats_file)
        logger.debug(f"Використовуємо кешований stats")
    
    # Отримуємо список аркушів з колонки "Аркуш" (пропускаємо де немає відвідування)
    sheets_to_download = []
    for _, row in stats_df.iterrows():
        sheet_name = str(row['Аркуш']).strip()
        entered = str(row['Зайшов']).strip()
        
        if not entered or entered.lower() in ['nan', 'none', '']:
            continue
        
        if sheet_name and sheet_name != 'nan':
            try:
                datetime.datetime.strptime(sheet_name, "%d.%m.%Y")
                sheets_to_download.append(sheet_name)
            except ValueError:
                continue
    
    # Визначаємо які аркуші потрібно (пере)завантажити
    # Завжди перезавантажуємо останні N днів (можуть бути оновлені адміністратором)
    REFRESH_LAST_N_DAYS = 5
    
    if force_refresh_all_sheets:
        # Режим повного перезавантаження - ігноруємо кеш
        logger.info("Режим force_refresh_all_sheets: ігнорування кешу для всіх аркушів")
        sheets_to_update = sheets_to_download
    else:
        cutoff_date = datetime.date.today() - datetime.timedelta(days=REFRESH_LAST_N_DAYS)
        
        existing_sheets = set()
        for filename in os.listdir(DAILY_SHEETS_CACHE_DIR):
            if filename.endswith('.csv') and filename != '_stats.csv':
                try:
                    date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
                    sheet_name = date_obj.strftime("%d.%m.%Y")
                    
                    # Додаємо тільки старі аркуші (які НЕ будемо оновлювати)
                    if date_obj < cutoff_date:
                        existing_sheets.add(sheet_name)
                except:
                    continue
        
        # Аркуші які відсутні АБО в межах останніх N днів
        sheets_to_update = [s for s in sheets_to_download if s not in existing_sheets]
    
    sheets_updated = False
    if sheets_to_update:
        logger.info(f"Завантаження {len(sheets_to_update)} аркушів (включно з оновленням останніх {REFRESH_LAST_N_DAYS} днів)...")
        for sheet_name in sheets_to_update:
            if download_daily_sheet(sheets_service, stats_sheet_id, sheet_name):
                sheets_updated = True
    
    # Якщо були оновлення або force_refresh_stats, регенеруємо attendance_data.json
    if sheets_updated or should_refresh:
        logger.info("Оновлення attendance_data.json...")
        generate_attendance_json()
    
    return True

def parse_daily_sheet_attendance(csv_file):
    """
    Парсить щоденний аркуш і повертає дані про ФАКТИЧНУ відвідуваність.
    Витягує ЛІВУ частину (Попередній прийом) - перші 4 колонки: №, ID, Статус, Примітки.
    
    Структура файлу:
    - Рядок 0: Заголовок
    - Рядок 1: "Попередній прийом:" ...
    - Рядок 2: Заголовки колонок (№, ID, Статус, Примітки, ..., №, ID, Статус, ...)
    - Рядок 3+: Дані
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    if len(rows) < 4:
        return None
    
    # Знаходимо рядок з заголовками (де перша колонка = '№')
    data_start_idx = None
    for i, row in enumerate(rows):
        if len(row) > 0 and row[0].strip() == '№':
            # Дані починаються з наступного рядка
            data_start_idx = i + 1
            break
    
    if data_start_idx is None:
        return None
    
    attended = 0
    no_show = 0
    postponed = 0
    total = 0
    
    # Парсимо ТІЛЬКИ ліву частину (колонки 0-3: №, ID, Статус, Примітки)
    for row in rows[data_start_idx:]:
        if len(row) < 3:
            continue
        
        # Ліва частина: Попередній прийом (ФАКТИЧНІ дані)
        number = row[0].strip()
        person_id = row[1].strip()
        status = row[2].strip()
        
        # Пропускаємо порожні рядки та нечислові номери
        if not number or not person_id:
            continue
        
        if not number.isdigit():
            continue
        
        try:
            int(person_id)
        except ValueError:
            continue
        
        total += 1
        status_lower = status.lower()
        
        if 'зайшов' in status_lower and 'не зайшов' not in status_lower and "не з'явився" not in status_lower:
            attended += 1
        elif 'не зайшов' in status_lower or "не з'явився" in status_lower:
            no_show += 1
        elif 'відклав' in status_lower:
            postponed += 1
    
    if total == 0:
        return None
    
    return {
        'total': total,
        'attended': attended,
        'no_show': no_show,
        'postponed': postponed,
        'attendance_rate': attended / total
    }

def get_attendance_rate(num_recent_days=10):
    """
    Розраховує медіанну відвідуваність на основі останніх N днів.
    Повертає число від 0 до 1.
    """
    files = sorted([f for f in os.listdir(DAILY_SHEETS_CACHE_DIR) 
                    if f.endswith('.csv') and f != '_stats.csv'])
    
    recent_files = files[-num_recent_days:] if len(files) >= num_recent_days else files
    
    rates = []
    for filename in recent_files:
        filepath = os.path.join(DAILY_SHEETS_CACHE_DIR, filename)
        data = parse_daily_sheet_attendance(filepath)
        if data:
            rates.append(data['attendance_rate'])
    
    # Використовуємо медіану для стійкості до викидів
    import statistics
    median_rate = statistics.median(rates)
    
    logger.info(f"Медіанна відвідуваність (останні {len(rates)} днів): {median_rate:.1%}")
    return median_rate

def extract_attended_ids_from_sheet(csv_file):
    """
    Витягує список ID людей які ЗАЙШЛИ (статус = "Зайшов") з щоденного аркуша.
    Повертає список словників з полями: {'id': int, 'is_live': bool}.
    is_live=True якщо статус містить "(за живою чергою)".
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    if len(rows) < 4:
        return []
    
    # Знаходимо початок даних
    data_start_idx = None
    for i, row in enumerate(rows):
        if len(row) > 0 and row[0].strip() == '№':
            data_start_idx = i + 1
            break
    
    if data_start_idx is None:
        return []
    
    attended_ids = []
    
    # Парсимо ліву частину (попередній прийом)
    for row in rows[data_start_idx:]:
        if len(row) < 3:
            continue
        
        number = row[0].strip()
        person_id = row[1].strip()
        status = row[2].strip()
        
        if not number or not person_id or not number.isdigit():
            continue
        
        try:
            id_val = int(person_id)
        except ValueError:
            continue
        
        # Тільки тих хто зайшов
        status_lower = status.lower()
        if 'зайшов' in status_lower and 'не зайшов' not in status_lower and "не з'явився" not in status_lower:
            # Перевіряємо чи це жива черга
            is_live = 'за живою чергою' in status_lower
            attended_ids.append({'id': id_val, 'is_live': is_live})
    
    return attended_ids

def get_historical_attendance_data():
    """
    Витягує історичні дані про фактичну відвідуваність з усіх щоденних аркушів.
    
    Повертає DataFrame з колонками:
    - date: дата прийому
    - attended_data: список об'єктів {'id': int, 'is_live': bool}
    - attended_ids: список ID які зайшли (для зворотної сумісності)
    - count: скільки людей зайшло
    - min_id: мінімальний ID що зайшов
    - max_id: максимальний ID що зайшов
    - avg_id: середній ID що зайшов
    """
    ensure_cache_dir()
    
    files = sorted([f for f in os.listdir(DAILY_SHEETS_CACHE_DIR) 
                    if f.endswith('.csv') and f != '_stats.csv'])
    
    if not files:
        logger.warning("Немає щоденних аркушів")
        return None
    
    import datetime
    import pandas as pd
    
    data = []
    for filename in files:
        try:
            date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
        except:
            continue
        
        filepath = os.path.join(DAILY_SHEETS_CACHE_DIR, filename)
        attended_data = extract_attended_ids_from_sheet(filepath)
        
        if attended_data:
            # attended_data тепер список об'єктів {'id': int, 'is_live': bool}
            attended_ids = [item['id'] for item in attended_data]
            data.append({
                'date': date_obj,
                'attended_data': attended_data,  # Зберігаємо повні дані з is_live
                'attended_ids': attended_ids,    # Для зворотної сумісності
                'count': len(attended_ids),
                'min_id': min(attended_ids),
                'max_id': max(attended_ids),
                'avg_id': sum(attended_ids) / len(attended_ids)
            })
    
    if not data:
        logger.warning("Не знайдено даних про відвідуваність")
        return None
    
    df = pd.DataFrame(data)
    df = df.sort_values('date')
    
    logger.info(f"Завантажено історичні дані про відвідуваність для {len(df)} днів")
    return df

def generate_attendance_json(output_file='attendance_data.json'):
    """
    Генерує attendance_data.json з усіма історичними точками відвідуваності.
    Використовує дані зі щоденних аркушів та mapping з Stats.
    
    Returns:
        bool: True якщо успішно, False якщо помилка
    """
    import json
    import datetime
    
    ensure_cache_dir()
    
    # Читаємо Stats для отримання mapping: назва аркуша -> дата прийому
    stats_file = os.path.join(DAILY_SHEETS_CACHE_DIR, "_stats.csv")
    if not os.path.exists(stats_file):
        logger.error("Файл _stats.csv не знайдено. Запустіть синхронізацію спочатку")
        return False
    
    stats_df = pd.read_csv(stats_file)
    
    # Створюємо mapping: назва аркуша -> дата прийому
    sheet_to_date = {}
    for _, row in stats_df.iterrows():
        sheet_name = str(row['Аркуш']).strip()
        date_str = str(row['Дата прийому']).strip()
        
        if sheet_name and sheet_name != 'nan' and date_str and date_str != 'nan':
            try:
                visit_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                sheet_to_date[sheet_name] = visit_date
            except ValueError:
                continue
    
    files = sorted([f for f in os.listdir(DAILY_SHEETS_CACHE_DIR) 
                    if f.endswith('.csv') and f != '_stats.csv'])
    
    if not files:
        logger.warning("Немає щоденних аркушів для обробки")
        return False
    
    attendance_points = []
    
    for filename in files:
        try:
            file_date_obj = datetime.datetime.strptime(filename[:-4], "%Y-%m-%d").date()
            sheet_name = file_date_obj.strftime("%d.%m.%Y")
        except:
            continue
        
        if sheet_name not in sheet_to_date:
            continue
        
        visit_date = sheet_to_date[sheet_name]
        filepath = os.path.join(DAILY_SHEETS_CACHE_DIR, filename)
        attended_data = extract_attended_ids_from_sheet(filepath)
        
        if attended_data:
            for person_data in attended_data:
                attendance_points.append({
                    'date': visit_date.strftime('%Y-%m-%d'),
                    'id': person_data['id'],
                    'is_live': person_data['is_live']
                })
    
    # Зберігаємо в JSON
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'attendance_points': attendance_points,
                'total_points': len(attendance_points)
            }, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Згенеровано {len(attendance_points)} точок у файл {output_file}")
        return True
    except Exception as e:
        logger.error(f"Помилка збереження {output_file}: {e}")
        return False

def load_attendance_from_json(json_file='attendance_data.json'):
    """
    Завантажує дані відвідуваності з JSON файлу.
    
    Returns:
        dict з attendance_points або None
    """
    try:
        import json
        if not os.path.exists(json_file):
            logger.warning(f"Файл {json_file} не знайдено")
            return None
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Завантажено {data.get('total_points', 0)} точок з {json_file}")
        return data
    except Exception as e:
        logger.error(f"Помилка завантаження {json_file}: {e}")
        return None

def calculate_prediction_from_attendance_json(user_id, attendance_data):
    """
    Розраховує прогноз на основі даних з attendance_data.json.
    
    Args:
        user_id: ID користувача
        attendance_data: дані з attendance_data.json
    
    Returns:
        dict з прогнозом або None
    """
    import numpy as np
    import datetime
    from scipy import stats as scipy_stats
    
    points = attendance_data.get('attendance_points', [])
    if len(points) < 5:
        return None
    
    # Функції для конвертації дати
    def get_ordinal_date(date_obj):
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
    
    # Створюємо список точок з ординалами
    processed_points = []
    for point in points:
        try:
            date_obj = datetime.datetime.strptime(point['date'], '%Y-%m-%d').date()
            ordinal = get_ordinal_date(date_obj)
            processed_points.append({
                'id': point['id'],
                'ordinal': ordinal,
                'is_live': point.get('is_live', False)
            })
        except:
            continue
    
    if len(processed_points) < 5:
        return None
    
    # Створюємо DataFrame
    points_df = pd.DataFrame(processed_points)
    
    # Групуємо за ID і беремо середню дату
    id_groups = points_df.groupby('id').agg({
        'ordinal': 'mean',
        'is_live': 'max'
    }).reset_index()
    daily_stats = id_groups.sort_values('ordinal')
    
    X = daily_stats['id'].values
    Y = daily_stats['ordinal'].values
    is_live_mask = daily_stats['is_live'].values
    n = len(X)
    
    if n < 5:
        return None
    
    # Експоненційні ваги (синхронізовано з index.html)
    weightExpMin = -3
    weightExpMax = 1
    liveQueueWeight = 0
    
    weights = np.exp(weightExpMin + (np.arange(n) / (n - 1)) * (weightExpMax - weightExpMin))
    weights = np.where(is_live_mask, weights * liveQueueWeight, weights)
    
    sumW = np.sum(weights)
    sumWX = np.sum(weights * X)
    sumWY = np.sum(weights * Y)
    sumWXX = np.sum(weights * X**2)
    sumWXY = np.sum(weights * X * Y)
    
    denom = sumW * sumWXX - sumWX**2
    if denom == 0:
        return None
    
    slope = (sumW * sumWXY - sumWX * sumWY) / denom
    intercept = (sumWY - slope * sumWX) / sumW
    
    weightedMeanX = sumWX / sumW
    weightedVarX = np.sum(weights * (X - weightedMeanX)**2)
    
    yPred = slope * X + intercept
    residuals = Y - yPred
    weightedSumResSq = np.sum(weights * residuals**2)
    
    dof = sumW - 2
    if dof <= 0:
        return None
    
    mseWeighted = weightedSumResSq / dof
    
    # T-показники
    tScore90 = scipy_stats.t.ppf(0.95, dof)
    tScore50 = scipy_stats.t.ppf(0.75, dof)
    
    predOrd = slope * user_id + intercept
    
    term3 = (user_id - weightedMeanX)**2 / weightedVarX
    sePred = np.sqrt(mseWeighted * (1 + 1/sumW + term3))
    
    margin90 = tScore90 * sePred
    margin50 = tScore50 * sePred
    
    l90_ord = predOrd - margin90
    h90_ord = predOrd + margin90
    l50_ord = predOrd - margin50
    h50_ord = predOrd + margin50
    
    # Обмеження майбутнім
    max_hist_ord = points_df['ordinal'].max()
    min_feasible = max_hist_ord + 1
    
    if user_id > daily_stats['id'].max():
        l90_ord = max(l90_ord, min_feasible)
        l50_ord = max(l50_ord, min_feasible)
    
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
        },
        'data_points': len(processed_points),
        'data_source': 'attendance_json'
    }

def calculate_prediction_with_daily_data(user_id, use_daily_sheets=True, use_json_cache=True):
    """
    Розраховує прогноз дати візиту використовуючи детальні дані зі щоденних аркушів.
    
    Замість припущення що зайшли всі ID від min до max,
    використовує реальні ID людей які зайшли.
    
    ВАЖЛИВО: Константи ваг синхронізовані з index.html:
    - weightExpMin, weightExpMax, liveQueueWeight
    Після зміни цих значень в index.html, оновіть їх тут також!
    
    Args:
        user_id: ID користувача
        use_daily_sheets: якщо True, використовує дані зі щоденних аркушів,
                          інакше None (буде використано stats)
        use_json_cache: якщо True, завантажує дані з attendance_data.json,
                        інакше парсить CSV файли безпосередньо
    
    Returns:
        dict з прогнозом або None
    """
    if not use_daily_sheets:
        return None
    
    # Спочатку пробуємо завантажити з JSON кешу
    if use_json_cache:
        attendance_data = load_attendance_from_json()
        if attendance_data:
            return calculate_prediction_from_attendance_json(user_id, attendance_data)
    
    # Fallback: завантажуємо з CSV файлів
    hist_df = get_historical_attendance_data()
    
    if hist_df is None or len(hist_df) < 5:
        return None
    
    import numpy as np
    import datetime
    from scipy import stats as scipy_stats
    
    # Для кожного дня створюємо окрему точку для кожного ID що зайшов
    points = []
    
    # Функція для конвертації дати в ordinal (як в оригінальному calculate_prediction)
    def get_ordinal_date(date_obj):
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
    
    # Створюємо окрему точку для кожного ID що зайшов
    # Це дає регресії повну інформацію про розкид і викиди
    for _, row in hist_df.iterrows():
        date_ordinal = get_ordinal_date(row['date'])
        for attended_item in row['attended_data']:
            points.append({
                'id': attended_item['id'],
                'ordinal': date_ordinal,
                'is_live': attended_item['is_live']
            })
    
    if len(points) < 5:
        return None
    
    # Створюємо DataFrame з усіх точок
    points_df = pd.DataFrame(points)
    
    # Групуємо за ID і беремо середню дату (якщо ID зайшов кілька разів)
    # Також маркуємо ID як is_live якщо хоча б одне відвідування було за живою чергою
    id_groups = points_df.groupby('id').agg({
        'ordinal': 'mean',
        'is_live': 'max'  # True якщо хоча б одне відвідування було is_live
    }).reset_index()
    daily_stats = id_groups.sort_values('ordinal')
    
    X = daily_stats['id'].values
    Y = daily_stats['ordinal'].values
    is_live_mask = daily_stats['is_live'].values
    n = len(X)
    
    if n < 5:
        return None
    
    # Експоненційні ваги (новіші дані важливіші) + зменшена вага для живої черги
    # Синхронізовано з index.html (константи WEIGHT_EXP_MIN, WEIGHT_EXP_MAX, LIVE_QUEUE_WEIGHT)
    weightExpMin = -3
    weightExpMax = 1
    liveQueueWeight = 0  # Виключаємо живу чергу з розрахунків
    
    weights = np.exp(weightExpMin + (np.arange(n) / (n - 1)) * (weightExpMax - weightExpMin))
    
    # Зменшуємо вагу для точок живої черги
    weights = np.where(is_live_mask, weights * liveQueueWeight, weights)
    
    sumW = np.sum(weights)
    sumWX = np.sum(weights * X)
    sumWY = np.sum(weights * Y)
    sumWXX = np.sum(weights * X**2)
    sumWXY = np.sum(weights * X * Y)
    
    denom = sumW * sumWXX - sumWX**2
    if denom == 0:
        return None
    
    slope = (sumW * sumWXY - sumWX * sumWY) / denom
    intercept = (sumWY - slope * sumWX) / sumW
    
    weightedMeanX = sumWX / sumW
    weightedVarX = np.sum(weights * (X - weightedMeanX)**2)
    
    yPred = slope * X + intercept
    residuals = Y - yPred
    weightedSumResSq = np.sum(weights * residuals**2)
    
    dof = sumW - 2
    if dof <= 0:
        return None
    
    mseWeighted = weightedSumResSq / dof
    
    # T-показники
    tScore90 = scipy_stats.t.ppf(0.95, dof)
    tScore50 = scipy_stats.t.ppf(0.75, dof)
    
    predOrd = slope * user_id + intercept
    
    term3 = (user_id - weightedMeanX)**2 / weightedVarX
    sePred = np.sqrt(mseWeighted * (1 + 1/sumW + term3))
    
    margin90 = tScore90 * sePred
    margin50 = tScore50 * sePred
    
    l90_ord = predOrd - margin90
    h90_ord = predOrd + margin90
    l50_ord = predOrd - margin50
    h50_ord = predOrd + margin50
    
    # Обмеження майбутнім
    max_hist_ord = points_df['ordinal'].max()
    min_feasible = max_hist_ord + 1
    
    if user_id > daily_stats['id'].max():
        l90_ord = max(l90_ord, min_feasible)
        l50_ord = max(l50_ord, min_feasible)
    
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
        },
        'data_points': len(points),
        'data_source': 'daily_sheets',
        'using_daily_sheets': True
    }
