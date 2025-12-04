"""
Скрипт для синхронізації даних в GitHub Actions.
"""

import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from daily_sheets_sync import sync_daily_sheets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATS_SHEET_ID = '1d9OG-0b7wxxqrOujC9v6ikhjMKL2ei3wfrfaG61zSjA'
STATS_WORKSHEET_NAME = 'Stats'
SERVICE_ACCOUNT_KEY_PATH = 'service_account_key.json'
SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def main():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_KEY_PATH, 
        scopes=SERVICE_ACCOUNT_SCOPES
    )
    sheets_service = build('sheets', 'v4', credentials=creds)
    
    success = sync_daily_sheets(
        sheets_service, 
        STATS_SHEET_ID, 
        STATS_WORKSHEET_NAME, 
        force_refresh_stats=True
    )
    
    if success:
        logger.info('Синхронізація успішна')
        return 0
    else:
        logger.error('Помилка синхронізації')
        return 1

if __name__ == '__main__':
    exit(main())

