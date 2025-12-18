#!/usr/bin/env python3
"""
Скрипт для перевірки ймовірності прийому для конкретного ID.
Використання: python3 check_probability.py <ID>
"""

import sys
import os
from datetime import datetime, timedelta

from admission_probability import (
    load_historical_stats,
    calculate_metrics,
    get_latest_csv,
    parse_right_section,
    get_working_days,
    calculate_admission_probability,
    fetch_todo_list,
    count_todo_entries_for_date
)


def main():
    if len(sys.argv) < 2:
        print("Використання: python3 check_probability.py <ID>")
        print("Приклад: python3 check_probability.py 4355")
        sys.exit(1)
    
    target_id = sys.argv[1].strip()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cache_dir = os.path.join(script_dir, 'daily_sheets_cache')
    
    historical_stats = load_historical_stats(cache_dir)
    metrics = calculate_metrics(historical_stats)
    
    print("Завантаження TODO списку...")
    todo_list = fetch_todo_list()
    print(f"Завантажено {len(todo_list)} записів TODO")
    
    latest_csv = get_latest_csv(cache_dir)
    if not latest_csv:
        print("Помилка: не знайдено CSV файлів")
        sys.exit(1)
    
    csv_date_str = os.path.basename(latest_csv).replace('.csv', '')
    base_date = datetime.strptime(csv_date_str, '%Y-%m-%d')
    
    queue = parse_right_section(latest_csv)
    
    target_entry = None
    for entry in queue:
        if entry.queue_id == target_id:
            target_entry = entry
            break
    
    if not target_entry:
        print(f"ID {target_id} не знайдено в поточній черзі")
        sys.exit(1)
    
    working_days = get_working_days(base_date + timedelta(days=1), 5)
    
    results = calculate_admission_probability(queue, metrics, todo_list, base_date, num_working_days=5)
    
    target_result = None
    for r in results:
        if r['queue_id'] == target_id:
            target_result = r
            break
    
    if not target_result:
        print(f"Помилка розрахунку для ID {target_id}")
        sys.exit(1)
    
    print()
    print("=" * 70)
    print(f"ЙМОВІРНІСТЬ ПРИЙОМУ ДЛЯ ID: {target_id}")
    print("=" * 70)
    print()
    print(f"Поточна позиція в черзі: {target_result['position']}")
    print()
    print("Прогноз по днях:")
    print("-" * 70)
    print(f"{'День':<25} | {'Поз.':<6} | {'Еф.поз':<7} | {'Шанс':<7} | TODO")
    print("-" * 70)
    
    day_positions = target_result.get('day_positions', [target_result['position']] * 5)
    day_eff_positions = target_result.get('day_effective_positions', [target_result['effective_position']] * 5)
    
    for i, wd in enumerate(working_days):
        prob = target_result['day_probabilities'][i]
        pos = day_positions[i] if i < len(day_positions) else '-'
        eff_pos = day_eff_positions[i] if i < len(day_eff_positions) else '-'
        
        day_name = wd.strftime('%d.%m.%Y (%A)')
        day_name = day_name.replace('Monday', 'Пн')
        day_name = day_name.replace('Tuesday', 'Вт')
        day_name = day_name.replace('Wednesday', 'Ср')
        day_name = day_name.replace('Thursday', 'Чт')
        day_name = day_name.replace('Friday', "Пт")
        
        todo_count = count_todo_entries_for_date(todo_list, wd)
        print(f"  {day_name:<23} | {pos:>5.0f} | {eff_pos:>6.1f} | {prob:>5.1f}% | {todo_count}")
    
    print("-" * 70)
    print()
    
    if target_result.get('notes'):
        print(f"Примітки: {target_result['notes']}")
    
    print()
    print("Статистика:")
    print(f"  - Середній прогрес за день: {metrics.get('avg_positions_processed', 0):.1f} позицій")
    print(f"  - Відсоток неявок: {metrics.get('no_show_rate', 0) * 100:.1f}%")
    print()
    print("Поз. = позиція в черзі на початок дня")
    print("Еф.поз = ефективна позиція (з урахуванням неявок)")
    print("Шанс = ймовірність бути прийнятим в цей конкретний день")
    print()


if __name__ == '__main__':
    main()

