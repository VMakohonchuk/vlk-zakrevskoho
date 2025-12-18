#!/usr/bin/env python3
"""
Розрахунок ймовірності прийому для черги ВЛК.

Алгоритм:
1. Для кожного дня рахуємо незалежну ймовірність (не кумулятивну)
2. Враховуємо TODO записи - людей записаних на конкретні дати
3. Якщо людина перенесе візит на день D, яка її позиція в черзі того дня?
"""

import csv
import os
import glob
import urllib.request
import io
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import statistics


SHEET_ID = '1d9OG-0b7wxxqrOujC9v6ikhjMKL2ei3wfrfaG61zSjA'
TODO_GID = '84071606'
TODO_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={TODO_GID}'


@dataclass
class DayStats:
    """Статистика за один день прийому."""
    date: str
    positions_processed: int
    entered_scheduled: int
    entered_live: int
    no_show_count: int
    postponed_count: int
    not_reached_count: int
    last_entered_id: Optional[str]


@dataclass
class QueueEntry:
    """Запис у черзі."""
    position: int
    queue_id: str
    status: str
    notes: str


@dataclass
class TodoEntry:
    """Запис з TODO списку."""
    seq_num: int
    queue_id: str
    scheduled_date: datetime
    notes: str


def fetch_todo_list() -> List[TodoEntry]:
    """
    Завантажує TODO список з Google Sheets.
    """
    entries = []
    
    try:
        req = urllib.request.Request(TODO_URL, headers={
            'User-Agent': 'Mozilla/5.0'
        })
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode('utf-8')
        
        reader = csv.reader(io.StringIO(content))
        lines = list(reader)
        
        for row in lines:
            if len(row) < 3:
                continue
            
            try:
                seq_str = row[0].strip() if row[0] else ''
                if not seq_str or not seq_str.isdigit():
                    continue
                
                seq_num = int(seq_str)
                queue_id = row[1].strip() if len(row) > 1 else ''
                date_str = row[2].strip() if len(row) > 2 else ''
                notes = row[3].strip() if len(row) > 3 else ''
                
                if not queue_id or not date_str:
                    continue
                
                try:
                    scheduled_date = datetime.strptime(date_str, '%d.%m.%Y')
                except ValueError:
                    continue
                
                entries.append(TodoEntry(
                    seq_num=seq_num,
                    queue_id=queue_id,
                    scheduled_date=scheduled_date,
                    notes=notes
                ))
            except (ValueError, IndexError):
                continue
        
        return entries
        
    except Exception as e:
        print(f"Помилка завантаження TODO: {e}")
        return []


def parse_left_section(csv_path: str) -> List[QueueEntry]:
    """
    Парсить ліву секцію CSV (результати попереднього дня).
    """
    entries = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        
        for row in lines:
            if len(row) < 3:
                continue
            
            try:
                pos_str = row[0].strip() if row[0] else ''
                if not pos_str or not pos_str.isdigit():
                    continue
                
                position = int(pos_str)
                queue_id = row[1].strip() if len(row) > 1 else ''
                status = row[2].strip() if len(row) > 2 else ''
                notes = row[3].strip() if len(row) > 3 else ''
                
                if position > 0 and queue_id:
                    entries.append(QueueEntry(
                        position=position,
                        queue_id=queue_id,
                        status=status,
                        notes=notes
                    ))
            except (ValueError, IndexError):
                continue
    
    return entries


def parse_right_section(csv_path: str) -> List[QueueEntry]:
    """
    Парсить праву секцію CSV (поточна/майбутня черга).
    """
    entries = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        
        for row in lines:
            if len(row) < 8:
                continue
            
            try:
                pos_str = row[5].strip() if row[5] else ''
                if not pos_str or not pos_str.isdigit():
                    continue
                
                position = int(pos_str)
                notes = row[6].strip() if len(row) > 6 else ''
                queue_id = row[7].strip() if len(row) > 7 else ''
                status = row[8].strip() if len(row) > 8 else ''
                
                if position > 0 and queue_id:
                    entries.append(QueueEntry(
                        position=position,
                        queue_id=queue_id,
                        status=status,
                        notes=notes
                    ))
            except (ValueError, IndexError):
                continue
    
    return entries


def analyze_day(entries: List[QueueEntry]) -> Optional[DayStats]:
    """
    Аналізує результати дня за коректним алгоритмом.
    """
    if not entries:
        return None
    
    first_30 = [e for e in entries if e.position <= 30]
    above_30 = [e for e in entries if e.position > 30]
    
    entered_in_30 = [e for e in first_30 if e.status == 'Зайшов']
    entered_live = [e for e in above_30 if 'за живою чергою' in e.status.lower()]
    
    if not entered_in_30:
        last_entered_pos = 0
        last_entered_id = None
    else:
        last_entered = max(entered_in_30, key=lambda x: x.position)
        last_entered_pos = last_entered.position
        last_entered_id = last_entered.queue_id
    
    no_show_count = 0
    for e in first_30:
        if e.position < last_entered_pos and e.status == "Не з'явився":
            no_show_count += 1
    
    postponed_count = len([e for e in first_30 if 'Відклав' in e.status])
    
    not_reached_count = len([e for e in first_30 
                             if e.position > last_entered_pos 
                             and 'Не зайшов' in e.status])
    
    return DayStats(
        date='',
        positions_processed=last_entered_pos,
        entered_scheduled=len(entered_in_30),
        entered_live=len(entered_live),
        no_show_count=no_show_count,
        postponed_count=postponed_count,
        not_reached_count=not_reached_count,
        last_entered_id=last_entered_id
    )


def load_historical_stats(cache_dir: str) -> List[DayStats]:
    """
    Завантажує історичну статистику з усіх CSV файлів.
    """
    stats = []
    csv_files = sorted(glob.glob(os.path.join(cache_dir, '*.csv')))
    
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        
        if filename.startswith('_'):
            continue
        
        try:
            date_str = filename.replace('.csv', '')
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            continue
        
        entries = parse_left_section(csv_path)
        day_stats = analyze_day(entries)
        
        if day_stats and day_stats.positions_processed > 0:
            day_stats.date = date_str
            stats.append(day_stats)
    
    return stats


def calculate_metrics(stats: List[DayStats]) -> Dict:
    """
    Розраховує ключові метрики на основі історичних даних.
    """
    if not stats:
        return {}
    
    positions_processed = [s.positions_processed for s in stats]
    entered_counts = [s.entered_scheduled for s in stats]
    no_show_counts = [s.no_show_count for s in stats]
    live_counts = [s.entered_live for s in stats]
    
    total_should_enter = sum(s.positions_processed for s in stats)
    total_no_show = sum(no_show_counts)
    
    no_show_rate = total_no_show / total_should_enter if total_should_enter > 0 else 0
    
    return {
        'avg_positions_processed': statistics.mean(positions_processed),
        'std_positions_processed': statistics.stdev(positions_processed) if len(positions_processed) > 1 else 0,
        'avg_entered': statistics.mean(entered_counts),
        'std_entered': statistics.stdev(entered_counts) if len(entered_counts) > 1 else 0,
        'no_show_rate': no_show_rate,
        'avg_live_entries': statistics.mean(live_counts),
        'total_days': len(stats),
        'min_positions': min(positions_processed),
        'max_positions': max(positions_processed),
    }


def get_working_days(start_date: datetime, num_days: int) -> List[datetime]:
    """
    Повертає список робочих днів (Пн-Пт) починаючи з вказаної дати.
    """
    working_days = []
    current = start_date
    
    while len(working_days) < num_days:
        if current.weekday() < 5:
            working_days.append(current)
        current += timedelta(days=1)
    
    return working_days


def count_todo_entries_for_date(todo_list: List[TodoEntry], target_date: datetime) -> int:
    """
    Рахує кількість записів у TODO на конкретну дату.
    """
    count = 0
    target_date_only = target_date.date()
    
    for entry in todo_list:
        if entry.scheduled_date.date() == target_date_only:
            count += 1
    
    return count


def count_todo_entries_before_date(todo_list: List[TodoEntry], target_date: datetime, 
                                    after_date: datetime = None) -> int:
    """
    Рахує TODO записи до цільової дати (не включно), але після after_date.
    """
    count = 0
    target_date_only = target_date.date()
    after_date_only = after_date.date() if after_date else None
    
    for entry in todo_list:
        entry_date = entry.scheduled_date.date()
        if entry_date < target_date_only:
            if after_date_only is None or entry_date > after_date_only:
                count += 1
    
    return count


def calculate_admission_probability(
    queue: List[QueueEntry],
    metrics: Dict,
    todo_list: List[TodoEntry],
    base_date: datetime,
    num_working_days: int = 5
) -> List[Dict]:
    """
    Розраховує НЕЗАЛЕЖНУ ймовірність прийому для кожного ID на кожен день.
    
    Логіка:
    - Черга рухається: кожен день обробляється ~15 позицій
    - Позиція на день D = поточна_позиція - (оброблено_до_дня_D)
    - Ймовірність зростає з часом бо позиція покращується
    """
    results = []
    
    avg_pos = metrics.get('avg_positions_processed', 14)
    std_pos = metrics.get('std_positions_processed', 6)
    no_show_rate = metrics.get('no_show_rate', 0.3)
    
    working_days = get_working_days(base_date + timedelta(days=1), num_working_days)
    
    for entry in queue:
        if not entry.queue_id:
            continue
        
        position = entry.position
        
        day_probabilities = []
        day_positions = []  # Позиція в черзі на кожен день
        day_effective_positions = []  # Ефективна позиція на кожен день
        
        for day_idx, target_day in enumerate(working_days):
            days_elapsed = day_idx + 1
            
            # Скільки позицій оброблено ДО цільового дня (не включаючи сам день)
            positions_processed_before = avg_pos * (days_elapsed - 1)
            
            # Позиція в черзі на початок цільового дня
            queue_position_on_day = max(1, position - positions_processed_before)
            
            # Ефективна позиція з урахуванням неявок
            effective_position = queue_position_on_day * (1 - no_show_rate)
            
            day_positions.append(round(queue_position_on_day, 1))
            day_effective_positions.append(round(effective_position, 1))
            
            # Ймовірність бути прийнятим в цей день
            if effective_position <= 0:
                prob = 1.0
            elif std_pos > 0:
                z_score = (avg_pos - effective_position) / std_pos
                prob = _normal_cdf(z_score)
            else:
                prob = 1.0 if avg_pos >= effective_position else 0.0
            
            prob = max(0.0, min(1.0, prob))
            day_probabilities.append(round(prob * 100, 1))
        
        effective_position_day1 = position * (1 - no_show_rate)
        days_to_process = effective_position_day1 / avg_pos if avg_pos > 0 else float('inf')
        
        results.append({
            'position': position,
            'queue_id': entry.queue_id,
            'effective_position': round(effective_position_day1, 1),
            'estimated_days': round(days_to_process, 1),
            'day_positions': day_positions,
            'day_effective_positions': day_effective_positions,
            'day_probabilities': day_probabilities,
            'notes': entry.notes
        })
    
    return results


def _normal_cdf(z: float) -> float:
    """
    Апроксимація функції розподілу нормального розподілу.
    """
    import math
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def format_results(results: List[Dict], metrics: Dict, working_days: List[datetime], 
                   todo_counts: Dict[str, int]) -> str:
    """
    Форматує результати для виводу.
    """
    lines = []
    lines.append("=" * 100)
    lines.append("РОЗРАХУНОК ЙМОВІРНОСТІ ПРИЙОМУ НА НАЙБЛИЖЧІ 5 РОБОЧИХ ДНІВ")
    lines.append("=" * 100)
    lines.append("")
    lines.append("Історична статистика:")
    lines.append(f"  - Всього днів проаналізовано: {metrics.get('total_days', 0)}")
    lines.append(f"  - Середня кількість оброблених позицій/день: {metrics.get('avg_positions_processed', 0):.1f}")
    lines.append(f"  - Стандартне відхилення: {metrics.get('std_positions_processed', 0):.1f}")
    lines.append(f"  - Мін/Макс позицій: {metrics.get('min_positions', 0)} / {metrics.get('max_positions', 0)}")
    lines.append(f"  - Відсоток неявок: {metrics.get('no_show_rate', 0) * 100:.1f}%")
    lines.append("")
    lines.append("TODO записи по днях:")
    for wd in working_days:
        day_str = wd.strftime('%d.%m')
        count = todo_counts.get(wd.strftime('%Y-%m-%d'), 0)
        lines.append(f"  - {day_str}: {count} записів")
    lines.append("")
    
    # Таблиця з ефективними позиціями та ймовірностями
    day_headers = [d.strftime('%d.%m') for d in working_days]
    
    lines.append("Формат: Ефективна позиція (Ймовірність%)")
    lines.append("")
    
    header = f"{'Поз':>4} | {'ID':>8} | "
    header += " | ".join([f"{d:^14}" for d in day_headers])
    header += " | Прим."
    
    lines.append("-" * len(header))
    lines.append(header)
    lines.append("-" * len(header))
    
    for r in results[:50]:
        eff_positions = r.get('day_effective_positions', [0]*5)
        probs = r['day_probabilities']
        
        # Формат: "7.3 (81%)"
        cells = []
        for eff, prob in zip(eff_positions, probs):
            cells.append(f"{eff:>5.1f} ({prob:>4.0f}%)")
        
        cell_str = " | ".join(cells)
        
        notes = r.get('notes', '')[:6]
        
        line = f"{r['position']:>4} | {r['queue_id']:>8} | {cell_str} | {notes}"
        lines.append(line)
    
    if len(results) > 50:
        lines.append(f"... та ще {len(results) - 50} записів")
    
    lines.append("-" * len(header))
    lines.append("")
    lines.append("Пояснення:")
    lines.append("  - Поз: поточна позиція в черзі")
    lines.append("  - Число перед дужками: ефективна позиція на той день (з урахуванням руху черги та неявок)")
    lines.append("  - Число в дужках: ймовірність бути прийнятим якщо прийти в ТОЙ день")
    lines.append("")
    
    return "\n".join(lines)


def get_latest_csv(cache_dir: str) -> Optional[str]:
    """
    Знаходить найновіший CSV файл у кеші.
    """
    csv_files = []
    
    for f in glob.glob(os.path.join(cache_dir, '*.csv')):
        filename = os.path.basename(f)
        if filename.startswith('_'):
            continue
        try:
            date_str = filename.replace('.csv', '')
            date = datetime.strptime(date_str, '%Y-%m-%d')
            csv_files.append((date, f))
        except ValueError:
            continue
    
    if not csv_files:
        return None
    
    csv_files.sort(key=lambda x: x[0], reverse=True)
    return csv_files[0][1]


def main():
    """
    Головна функція для розрахунку ймовірностей.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cache_dir = os.path.join(script_dir, 'daily_sheets_cache')
    
    print("Завантаження історичних даних...")
    historical_stats = load_historical_stats(cache_dir)
    print(f"Завантажено {len(historical_stats)} днів статистики")
    
    metrics = calculate_metrics(historical_stats)
    
    print("Завантаження TODO списку з Google Sheets...")
    todo_list = fetch_todo_list()
    print(f"Завантажено {len(todo_list)} записів TODO")
    
    latest_csv = get_latest_csv(cache_dir)
    if not latest_csv:
        print("Помилка: не знайдено CSV файлів")
        return
    
    csv_date_str = os.path.basename(latest_csv).replace('.csv', '')
    base_date = datetime.strptime(csv_date_str, '%Y-%m-%d')
    print(f"Аналіз файлу: {os.path.basename(latest_csv)} (база: {base_date.strftime('%d.%m.%Y')})")
    
    queue = parse_right_section(latest_csv)
    print(f"Знайдено {len(queue)} записів у черзі")
    
    working_days = get_working_days(base_date + timedelta(days=1), 5)
    
    todo_counts = {}
    for wd in working_days:
        key = wd.strftime('%Y-%m-%d')
        todo_counts[key] = count_todo_entries_for_date(todo_list, wd)
    
    results = calculate_admission_probability(queue, metrics, todo_list, base_date, num_working_days=5)
    
    output = format_results(results, metrics, working_days, todo_counts)
    print(output)
    
    output_file = os.path.join(script_dir, 'probability_report.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output)
    print(f"\nЗвіт збережено: {output_file}")


if __name__ == '__main__':
    main()
