
import asyncio
import uuid
import uvicorn
import os
import json
import sqlite3
import csv
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
import logging

# Конфигурация из переменных окружения
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Инициализируем бота и диспетчер
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

class DatabaseManager:
    """Управление базой данных SQLite"""
    
    def __init__(self, db_path: str = "applications.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS applications (
                    id TEXT PRIMARY KEY,
                    form_name TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP,
                    approved_at TIMESTAMP,
                    data TEXT,
                    backup_file TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    total_received INTEGER DEFAULT 0,
                    total_approved INTEGER DEFAULT 0,
                    total_rejected INTEGER DEFAULT 0,
                    startup_forms INTEGER DEFAULT 0,
                    student_forms INTEGER DEFAULT 0
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ База данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    def save_application(self, app_id: str, form_name: str, data: dict, backup_file: str = None):
        """Сохранение заявки в БД"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO applications 
                (id, form_name, status, created_at, data, backup_file)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (app_id, form_name, 'pending', datetime.now(), json.dumps(data), backup_file))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
    
    def update_application_status(self, app_id: str, status: str):
        """Обновление статуса заявки"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            approved_at = datetime.now() if status == 'approved' else None
            cursor.execute('''
                UPDATE applications 
                SET status = ?, approved_at = ?
                WHERE id = ?
            ''', (status, approved_at, app_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса: {e}")
    
    def get_pending_applications(self) -> List[dict]:
        """Получение заявок в ожидании"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, form_name, data, created_at 
                FROM applications 
                WHERE status = 'pending'
                ORDER BY created_at DESC
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'form_name': row[1],
                    'data': json.loads(row[2]),
                    'created_at': row[3]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения заявок: {e}")
            return []
    
    def get_application(self, app_id: str) -> Optional[dict]:
        """Получение заявки по ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, form_name, data, status, created_at 
                FROM applications 
                WHERE id = ?
            ''', (app_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'id': row[0],
                    'form_name': row[1],
                    'data': json.loads(row[2]),
                    'status': row[3],
                    'created_at': row[4]
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения заявки: {e}")
            return None
    
    def cleanup_old_applications(self, days_old: int = 30):
        """Удаление старых заявок"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            cursor.execute('''
                DELETE FROM applications 
                WHERE created_at < ? AND status != 'pending'
            ''', (cutoff_date,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"🗑️ Удалено {deleted_count} старых заявок")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки старых заявок: {e}")
            return 0

    def get_statistics(self) -> dict:
        """Получение статистики"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Общая статистика
            cursor.execute('SELECT COUNT(*) FROM applications')
            total_received = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM applications WHERE status = "approved"')
            total_approved = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM applications WHERE status = "rejected"')
            total_rejected = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM applications WHERE status = "pending"')
            pending = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM applications WHERE form_name = "Стартап"')
            startup_forms = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM applications WHERE form_name = "Студент"')
            student_forms = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_received': total_received,
                'total_approved': total_approved,
                'total_rejected': total_rejected,
                'pending': pending,
                'startup_forms': startup_forms,
                'student_forms': student_forms
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_received': 0,
                'total_approved': 0,
                'total_rejected': 0,
                'pending': 0,
                'startup_forms': 0,
                'student_forms': 0
            }
    
    def export_to_csv(self, status: str = None) -> str:
        """Экспорт данных в CSV"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = 'SELECT * FROM applications'
            if status:
                query += ' WHERE status = ?'
                cursor.execute(query, (status,))
            else:
                cursor.execute(query)
            
            filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ID', 'Form Name', 'Status', 'Created At', 'Approved At', 'Data'])
                writer.writerows(cursor.fetchall())
            
            conn.close()
            return filename
            
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
            return None

# Инициализируем менеджер БД
db_manager = DatabaseManager()

def generate_application_id() -> str:
    """Генерирует уникальный ID для заявки"""
    return str(uuid.uuid4())[:8]

def validate_webhook_data(data: dict) -> bool:
    """Валидирует структуру данных webhook"""
    try:
        required_fields = ['eventType', 'data']
        if not all(field in data for field in required_fields):
            logger.warning(f"Отсутствуют обязательные поля: {required_fields}")
            return False
            
        if data['eventType'] != 'FORM_RESPONSE':
            logger.warning(f"Неожиданный тип события: {data['eventType']}")
            return False
            
        if 'fields' not in data.get('data', {}):
            logger.warning("Отсутствуют поля формы")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Ошибка валидации данных: {e}")
        return False

def backup_application(app_id: str, data: dict) -> str:
    """Сохраняет заявку в файл для резервного копирования"""
    try:
        backup_data = {
            "id": app_id,
            "timestamp": datetime.now().isoformat(),
            "data": data
        }
        filename = f"backup_{app_id}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Создана резервная копия заявки #{app_id}")
        return filename
    except Exception as e:
        logger.error(f"Ошибка создания резервной копии: {e}")
        return None

def format_application_message(data: dict, app_id: str) -> str:
    """Форматирует сообщение с заявкой для админа"""
    try:
        message = f"📋 <b>Новая заявка #{app_id}</b>\n\n"

        # Получаем поля из структуры Tally
        fields = data.get('data', {}).get('fields', [])
        form_name = data.get('data', {}).get('formName', 'Неизвестная форма')

        message += f"📝 <b>Тип формы:</b> {form_name}\n"
        message += f"🕐 <b>Время:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}\n\n"

        # Обрабатываем каждое поле
        processed_fields = 0
        for field in fields:
            label = field.get('label', '')
            value = field.get('value', '')
            field_type = field.get('type', '')

            # Пропускаем только пустые значения, False, пустые списки и дублированные поля чекбоксов
            if not value or value in [False, []] or (field.get('key', '').count('_') > 1):
                continue

            # Обрабатываем разные типы полей
            if field_type == 'CHECKBOXES' and isinstance(value, list):
                # Для чекбоксов извлекаем текст выбранных опций
                options = field.get('options', [])
                selected_texts = []
                for option_id in value:
                    for option in options:
                        if option.get('id') == option_id:
                            selected_texts.append(option.get('text', ''))
                if selected_texts:
                    value = ', '.join(selected_texts)

            if value and value != False:
                # Добавляем иконки для разных типов полей
                icon = "📌"
                if "имя" in label.lower() or "название" in label.lower():
                    icon = "👤"
                elif "email" in label.lower() or "почта" in label.lower():
                    icon = "📧"
                elif "телефон" in label.lower():
                    icon = "📱"
                elif "telegram" in label.lower():
                    icon = "💬"
                elif "город" in label.lower() or "страна" in label.lower():
                    icon = "🌍"
                elif "уровень" in label.lower():
                    icon = "⭐"
                elif "сотрудничество" in label.lower() or "тип" in label.lower():
                    icon = "🤝"
                elif "опыт" in label.lower() or "навыки" in label.lower():
                    icon = "💼"
                elif "описание" in label.lower() or "о себе" in label.lower():
                    icon = "📝"
                elif "ключевые" in label.lower():
                    icon = "🔑"

                message += f"{icon} <b>{label}:</b> {value}\n"
                processed_fields += 1

        if processed_fields == 0:
            message += "⚠️ <i>Данные формы не обнаружены</i>\n"

        message += f"\n⚡ Для одобрения: /approve {app_id}"
        message += f"\n❌ Для отклонения: /reject {app_id}"

        return message
        
    except Exception as e:
        logger.error(f"Ошибка форматирования сообщения: {e}")
        return f"📋 <b>Ошибка обработки заявки #{app_id}</b>\n\n❌ Ошибка форматирования данных"

def format_public_message(data: dict) -> str:
    """Форматирует сообщение для публикации в канале"""
    try:
        fields = data.get('data', {}).get('fields', [])
        form_name = data.get('data', {}).get('formName', '')

        if form_name == "Студент":
            message = "🎓 <b>Ищет работу/стажировку</b>\n\n"
        elif form_name == "Стартап":
            message = "🚀 <b>Ищет сотрудников</b>\n\n"
        else:
            message = "📋 <b>Новая заявка</b>\n\n"

        # Словарь для маппинга полей
        field_map = {}
        for field in fields:
            label = field.get('label', '').lower()
            value = field.get('value', '')
            field_type = field.get('type', '')

            # Пропускаем только пустые значения, False, пустые списки и дублированные поля чекбоксов
            if not value or value in [False, []] or (field.get('key', '').count('_') > 1):
                continue

            # Обрабатываем чекбоксы
            if field_type == 'CHECKBOXES' and isinstance(value, list):
                options = field.get('options', [])
                selected_texts = []
                for option_id in value:
                    for option in options:
                        if option.get('id') == option_id:
                            selected_texts.append(option.get('text', ''))
                if selected_texts:
                    value = ', '.join(selected_texts)

            if value and value != False:
                field_map[label] = value

        # Форматируем сообщение в зависимости от типа формы
        if form_name == "Студент":
            if 'имя и фамилия' in field_map:
                message += f"👤 <b>Имя:</b> {field_map['имя и фамилия']}\n"
            if 'кем вы хотите быть?' in field_map:
                message += f"🎯 <b>Специальность:</b> {field_map['кем вы хотите быть?']}\n"
            if 'уровень ваших умений' in field_map:
                message += f"⭐ <b>Уровень:</b> {field_map['уровень ваших умений']}\n"
            if 'с чем работаете/работали' in field_map:
                message += f"💼 <b>Технологии:</b> {field_map['с чем работаете/работали']}\n"
            if 'немного о себе?' in field_map:
                message += f"📝 <b>О себе:</b> {field_map['немного о себе?']}\n"
            if 'город/страна' in field_map:
                message += f"🌍 <b>Локация:</b> {field_map['город/страна']}\n"

            # Контакты
            if 'e-mail' in field_map:
                message += f"\n📧 <b>Контакт:</b> {field_map['e-mail']}"
            if 'telegram' in field_map:
                message += f"\n💬 <b>Telegram:</b> {field_map['telegram']}"

        elif form_name == "Стартап":
            if 'название/имя' in field_map:
                message += f"🏢 <b>Компания:</b> {field_map['название/имя']}\n"
            if 'кого ищите?' in field_map:
                message += f"🎯 <b>Ищет:</b> {field_map['кого ищите?']}\n"
            if 'тип сотрудничества' in field_map:
                message += f"🤝 <b>Тип работы:</b> {field_map['тип сотрудничества']}\n"
            if 'желаемый уровень кандидата' in field_map:
                message += f"⭐ <b>Уровень:</b> {field_map['желаемый уровень кандидата']}\n"
            if 'опишите работу/проект и задачи' in field_map:
                message += f"📝 <b>Описание:</b> {field_map['опишите работу/проект и задачи']}\n"
            if 'ключевые слова' in field_map:
                message += f"🔑 <b>Ключевые навыки:</b> {field_map['ключевые слова']}\n"
            if 'город/страна' in field_map:
                message += f"🌍 <b>Локация:</b> {field_map['город/страна']}\n"

            # Контакты
            if 'e-mail' in field_map:
                message += f"\n📧 <b>Контакт:</b> {field_map['e-mail']}"
            if 'telegram(если есть)' in field_map:
                message += f"\n💬 <b>Telegram:</b> {field_map['telegram(если есть)']}"

        return message
        
    except Exception as e:
        logger.error(f"Ошибка форматирования публичного сообщения: {e}")
        return "❌ Ошибка обработки заявки"

async def send_notification_to_admin(message: str, reply_markup: InlineKeyboardMarkup = None):
    """Отправка уведомления админу с обработкой ошибок"""
    try:
        await bot.send_message(ADMIN_ID, message, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления админу: {e}")

@dp.message(Command("start"))
async def start_handler(message: Message):
    """Обработчик команды /start"""
    try:
        if message.from_user.id == ADMIN_ID:
            stats = db_manager.get_statistics()
            stats_message = (
                f"📊 <b>Статистика:</b>\n"
                f"• Всего получено: {stats['total_received']}\n"
                f"• Одобрено: {stats['total_approved']}\n"
                f"• Отклонено: {stats['total_rejected']}\n"
                f"• В ожидании: {stats['pending']}\n"
                f"• Стартапы: {stats['startup_forms']}\n"
                f"• Студенты: {stats['student_forms']}"
            )
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📊 Подробная статистика", callback_data="detailed_stats")],
                [InlineKeyboardButton(text="📋 Список ожидающих", callback_data="pending_list")],
                [InlineKeyboardButton(text="📥 Экспорт данных", callback_data="export_menu")]
            ])
            
            await message.answer(
                "👋 Привет, админ! Бот готов к работе.\n\n"
                "📋 <b>Основные команды:</b>\n"
                "• /approve <ID> - одобрить заявку\n"
                "• /reject <ID> - отклонить заявку\n"
                "• /stats - показать статистику\n"
                "• /pending - список ожидающих заявок\n\n"
                "⚡ <b>Массовые операции:</b>\n"
                "• /bulk - массовое управление заявками\n"
                "• /cleanup - очистить старые заявки\n\n"
                "📊 <b>Данные:</b>\n"
                "• /export - экспорт данных\n"
                "• /restore - восстановить из бэкапа\n\n"
                f"{stats_message}\n\n"
                "🔗 Заявки приходят автоматически через webhook.",
                reply_markup=keyboard
            )
        else:
            await message.answer("🤖 Это служебный бот для обработки заявок.")
    except Exception as e:
        logger.error(f"Ошибка в start_handler: {e}")
        await message.answer("❌ Произошла ошибка при обработке команды.")

@dp.message(Command("stats"))
async def stats_handler(message: Message):
    """Обработчик команды /stats"""
    if message.from_user.id != ADMIN_ID:
        return
        
    try:
        stats = db_manager.get_statistics()
        stats_message = (
            f"📊 <b>Подробная статистика:</b>\n\n"
            f"📥 <b>Получено заявок:</b> {stats['total_received']}\n"
            f"✅ <b>Одобрено:</b> {stats['total_approved']}\n"
            f"❌ <b>Отклонено:</b> {stats['total_rejected']}\n"
            f"⏳ <b>В ожидании:</b> {stats['pending']}\n\n"
            f"📊 <b>По типам форм:</b>\n"
            f"🚀 Стартапы: {stats['startup_forms']}\n"
            f"🎓 Студенты: {stats['student_forms']}\n\n"
            f"📈 <b>Процент одобрения:</b> {(stats['total_approved'] / max(stats['total_received'], 1) * 100):.1f}%\n"
            f"🕐 <b>Время работы:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}"
        )
        await message.answer(stats_message)
    except Exception as e:
        logger.error(f"Ошибка в stats_handler: {e}")
        await message.answer("❌ Ошибка получения статистики.")

@dp.message(Command("pending"))
async def pending_handler(message: Message):
    """Обработчик команды /pending"""
    if message.from_user.id != ADMIN_ID:
        return
        
    try:
        pending_apps = db_manager.get_pending_applications()
        
        if not pending_apps:
            await message.answer("📭 Нет заявок в ожидании.")
            return
            
        pending_list = "⏳ <b>Заявки в ожидании:</b>\n\n"
        for app in pending_apps[:10]:  # Показываем только первые 10
            created_time = datetime.fromisoformat(app['created_at']).strftime('%d.%m %H:%M')
            pending_list += f"• #{app['id']} - {app['form_name']} ({created_time})\n"
            
        if len(pending_apps) > 10:
            pending_list += f"\n... и еще {len(pending_apps) - 10} заявок"
            
        await message.answer(pending_list)
    except Exception as e:
        logger.error(f"Ошибка в pending_handler: {e}")
        await message.answer("❌ Ошибка получения списка заявок.")

@dp.message(Command("export"))
async def export_handler(message: Message):
    """Обработчик команды /export"""
    if message.from_user.id != ADMIN_ID:
        return
        
    try:
        filename = db_manager.export_to_csv()
        if filename:
            with open(filename, 'rb') as file:
                await bot.send_document(ADMIN_ID, file, caption="📄 Экспорт всех заявок")
            os.remove(filename)  # Удаляем временный файл
        else:
            await message.answer("❌ Ошибка экспорта данных.")
    except Exception as e:
        logger.error(f"Ошибка в export_handler: {e}")
        await message.answer("❌ Ошибка экспорта данных.")

@dp.message(Command("cleanup"))
async def cleanup_handler(message: Message):
    """Обработчик команды очистки старых заявок"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        deleted_count = db_manager.cleanup_old_applications(30)
        await message.answer(f"🗑️ Очистка завершена. Удалено заявок: {deleted_count}")
    except Exception as e:
        logger.error(f"Ошибка очистки: {e}")
        await message.answer("❌ Ошибка очистки данных.")

@dp.message(Command("bulk"))
async def bulk_handler(message: Message):
    """Обработчик команды массовых операций"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        pending_apps = db_manager.get_pending_applications()
        
        if not pending_apps:
            await message.answer("📭 Нет заявок для массовых операций.")
            return
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Одобрить все студенческие", callback_data="bulk_approve_students")],
            [InlineKeyboardButton(text="✅ Одобрить все стартапы", callback_data="bulk_approve_startups")],
            [InlineKeyboardButton(text="❌ Отклонить старше 7 дней", callback_data="bulk_reject_old")]
        ])
        
        await message.answer(
            f"⚡ <b>Массовые операции</b>\n\n"
            f"📊 Заявок в ожидании: {len(pending_apps)}\n"
            f"🎓 Студентов: {len([app for app in pending_apps if app['form_name'] == 'Студент'])}\n"
            f"🚀 Стартапов: {len([app for app in pending_apps if app['form_name'] == 'Стартап'])}\n\n"
            f"Выберите действие:",
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка bulk операций: {e}")
        await message.answer("❌ Ошибка массовых операций.")

@dp.message(Command("restore"))
async def restore_handler(message: Message):
    """Обработчик команды /restore"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        # Ищем файлы бэкапов
        backup_files = [f for f in os.listdir('.') if f.startswith('backup_') and f.endswith('.json')]
        
        if not backup_files:
            await message.answer("📁 Файлы бэкапов не найдены.")
            return
        
        restored_count = 0
        for backup_file in backup_files:
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                
                app_id = backup_data['id']
                if not db_manager.get_application(app_id):
                    form_name = backup_data['data'].get('data', {}).get('formName', 'Unknown')
                    db_manager.save_application(app_id, form_name, backup_data['data'], backup_file)
                    restored_count += 1
            except Exception as e:
                logger.error(f"Ошибка восстановления {backup_file}: {e}")
        
        await message.answer(f"♻️ Восстановлено заявок: {restored_count}")
        
    except Exception as e:
        logger.error(f"Ошибка в restore_handler: {e}")
        await message.answer("❌ Ошибка восстановления данных.")

@dp.message(Command("approve"))
async def approve_handler(message: Message):
    """Обработчик команды /approve"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    try:
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.answer("❌ Укажите ID заявки. Пример: /approve abc123")
            return
            
        app_id = command_parts[1]
    except (IndexError, ValueError):
        await message.answer("❌ Неверный формат команды. Пример: /approve abc123")
        return

    application = db_manager.get_application(app_id)
    if not application or application['status'] != 'pending':
        await message.answer(f"❌ Заявка #{app_id} не найдена в ожидающих.")
        return

    try:
        # Публикуем в канал
        public_message = format_public_message(application['data'])
        await bot.send_message(CHANNEL_ID, public_message)

        # Обновляем статус в БД
        db_manager.update_application_status(app_id, 'approved')

        await message.answer(f"✅ Заявка #{app_id} одобрена и опубликована в канале!")
        logger.info(f"Заявка #{app_id} одобрена и опубликована")

    except Exception as e:
        logger.error(f"Ошибка при публикации заявки #{app_id}: {e}")
        await message.answer(f"❌ Ошибка при публикации заявки: {str(e)}")

@dp.message(Command("reject"))
async def reject_handler(message: Message):
    """Обработчик команды /reject"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    try:
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.answer("❌ Укажите ID заявки. Пример: /reject abc123")
            return
            
        app_id = command_parts[1]
    except (IndexError, ValueError):
        await message.answer("❌ Неверный формат команды. Пример: /reject abc123")
        return

    application = db_manager.get_application(app_id)
    if not application or application['status'] != 'pending':
        await message.answer(f"❌ Заявка #{app_id} не найдена в ожидающих.")
        return

    try:
        # Обновляем статус в БД
        db_manager.update_application_status(app_id, 'rejected')

        await message.answer(f"❌ Заявка #{app_id} отклонена.")
        logger.info(f"Заявка #{app_id} отклонена")
        
    except Exception as e:
        logger.error(f"Ошибка при отклонении заявки #{app_id}: {e}")
        await message.answer(f"❌ Ошибка при отклонении заявки: {str(e)}")

@dp.callback_query(lambda c: c.data and c.data.startswith('approve_'))
async def approve_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик inline-кнопки одобрения"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    try:
        app_id = callback_query.data.split('_')[1]
        
        application = db_manager.get_application(app_id)
        if not application or application['status'] != 'pending':
            await callback_query.answer(f"❌ Заявка #{app_id} уже обработана или не найдена.", show_alert=True)
            return

        # Публикуем в канал
        public_message = format_public_message(application['data'])
        await bot.send_message(CHANNEL_ID, public_message)

        # Обновляем статус в БД
        db_manager.update_application_status(app_id, 'approved')

        # Обновляем сообщение с кнопками
        new_text = callback_query.message.text + f"\n\n✅ <b>ОДОБРЕНО</b> ({datetime.now().strftime('%H:%M %d.%m.%Y')})"
        await callback_query.message.edit_text(new_text, reply_markup=None)
        
        await callback_query.answer("✅ Заявка одобрена и опубликована!", show_alert=False)
        logger.info(f"Заявка #{app_id} одобрена через inline-кнопку")

    except Exception as e:
        logger.error(f"Ошибка при одобрении заявки #{app_id} через кнопку: {e}")
        await callback_query.answer(f"❌ Ошибка при публикации: {str(e)}", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('reject_'))
async def reject_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик inline-кнопки отклонения"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    try:
        app_id = callback_query.data.split('_')[1]
        
        application = db_manager.get_application(app_id)
        if not application or application['status'] != 'pending':
            await callback_query.answer(f"❌ Заявка #{app_id} уже обработана или не найдена.", show_alert=True)
            return

        # Обновляем статус в БД
        db_manager.update_application_status(app_id, 'rejected')

        # Обновляем сообщение с кнопками
        new_text = callback_query.message.text + f"\n\n❌ <b>ОТКЛОНЕНО</b> ({datetime.now().strftime('%H:%M %d.%m.%Y')})"
        await callback_query.message.edit_text(new_text, reply_markup=None)
        
        await callback_query.answer("❌ Заявка отклонена", show_alert=False)
        logger.info(f"Заявка #{app_id} отклонена через inline-кнопку")

    except Exception as e:
        logger.error(f"Ошибка при отклонении заявки #{app_id} через кнопку: {e}")
        await callback_query.answer(f"❌ Ошибка при отклонении: {str(e)}", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('details_'))
async def details_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик кнопки 'Подробнее'"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для просмотра деталей.", show_alert=True)
        return

    try:
        app_id = callback_query.data.split('_')[1]
        
        application = db_manager.get_application(app_id)
        if not application:
            await callback_query.answer(f"❌ Заявка #{app_id} не найдена.", show_alert=True)
            return

        # Формируем подробное сообщение
        details_message = f"📊 <b>Подробная информация о заявке #{app_id}</b>\n\n"
        details_message += f"📝 <b>Форма:</b> {application['form_name']}\n"
        details_message += f"📅 <b>Получена:</b> {datetime.fromisoformat(application['created_at']).strftime('%H:%M %d.%m.%Y')}\n"
        details_message += f"📊 <b>Статус:</b> {application['status']}\n\n"
        
        # Добавляем все поля из формы
        fields = application['data'].get('data', {}).get('fields', [])
        for field in fields:
            label = field.get('label', '')
            value = field.get('value', '')
            field_type = field.get('type', '')
            
            if not value or value in [False, []] or (field.get('key', '').count('_') > 1):
                continue
                
            if field_type == 'CHECKBOXES' and isinstance(value, list):
                options = field.get('options', [])
                selected_texts = []
                for option_id in value:
                    for option in options:
                        if option.get('id') == option_id:
                            selected_texts.append(option.get('text', ''))
                if selected_texts:
                    value = ', '.join(selected_texts)
            
            if value and value != False:
                details_message += f"• <b>{label}:</b> {value}\n"

        # Создаем кнопки для быстрых действий
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{app_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{app_id}")
            ] if application['status'] == 'pending' else []
        ])

        await callback_query.message.answer(details_message, reply_markup=keyboard)
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка при показе деталей заявки #{app_id}: {e}")
        await callback_query.answer(f"❌ Ошибка при загрузке деталей", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('detailed_stats'))
async def detailed_stats_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик кнопки подробной статистики"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для просмотра статистики.", show_alert=True)
        return

    try:
        stats = db_manager.get_statistics()
        
        # Получаем дополнительную информацию
        pending_apps = db_manager.get_pending_applications()
        
        detailed_message = f"📊 <b>Подробная статистика системы</b>\n\n"
        detailed_message += f"📈 <b>Общие показатели:</b>\n"
        detailed_message += f"• Всего заявок: {stats['total_received']}\n"
        detailed_message += f"• Одобрено: {stats['total_approved']} ({(stats['total_approved'] / max(stats['total_received'], 1) * 100):.1f}%)\n"
        detailed_message += f"• Отклонено: {stats['total_rejected']} ({(stats['total_rejected'] / max(stats['total_received'], 1) * 100):.1f}%)\n"
        detailed_message += f"• В ожидании: {stats['pending']}\n\n"
        
        detailed_message += f"📊 <b>По типам форм:</b>\n"
        detailed_message += f"🚀 Стартапы: {stats['startup_forms']}\n"
        detailed_message += f"🎓 Студенты: {stats['student_forms']}\n\n"
        
        if pending_apps:
            detailed_message += f"⏰ <b>Последние заявки в ожидании:</b>\n"
            for app in pending_apps[:5]:
                created_time = datetime.fromisoformat(app['created_at']).strftime('%d.%m %H:%M')
                detailed_message += f"• #{app['id']} - {app['form_name']} ({created_time})\n"
            if len(pending_apps) > 5:
                detailed_message += f"... и еще {len(pending_apps) - 5} заявок\n"
        
        detailed_message += f"\n🕐 <b>Обновлено:</b> {datetime.now().strftime('%H:%M %d.%m.%Y')}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="detailed_stats")],
            [InlineKeyboardButton(text="📋 Список ожидающих", callback_data="pending_list")]
        ])
        
        await callback_query.message.edit_text(detailed_message, reply_markup=keyboard)
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка при показе подробной статистики: {e}")
        await callback_query.answer("❌ Ошибка при загрузке статистики", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('pending_list'))
async def pending_list_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик кнопки списка ожидающих заявок"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для просмотра списка.", show_alert=True)
        return

    try:
        pending_apps = db_manager.get_pending_applications()
        
        if not pending_apps:
            await callback_query.answer("📭 Нет заявок в ожидании", show_alert=True)
            return
        
        pending_message = f"⏳ <b>Заявки в ожидании ({len(pending_apps)}):</b>\n\n"
        
        for i, app in enumerate(pending_apps[:10], 1):
            created_time = datetime.fromisoformat(app['created_at']).strftime('%d.%m %H:%M')
            # Получаем первое значимое поле для краткого описания
            fields = app['data'].get('data', {}).get('fields', [])
            brief_info = ""
            for field in fields:
                if field.get('label', '').lower() in ['имя и фамилия', 'название/имя', 'имя'] and field.get('value'):
                    brief_info = f" - {field['value'][:20]}{'...' if len(field['value']) > 20 else ''}"
                    break
            
            pending_message += f"{i}. #{app['id']} - {app['form_name']} ({created_time}){brief_info}\n"
        
        if len(pending_apps) > 10:
            pending_message += f"\n... и еще {len(pending_apps) - 10} заявок"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data="pending_list")],
            [InlineKeyboardButton(text="📊 Назад к статистике", callback_data="detailed_stats")]
        ])
        
        await callback_query.message.edit_text(pending_message, reply_markup=keyboard)
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка при показе списка ожидающих: {e}")
        await callback_query.answer("❌ Ошибка при загрузке списка", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('export_menu'))
async def export_menu_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик меню экспорта"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для экспорта.", show_alert=True)
        return

    try:
        export_message = "📄 <b>Экспорт данных</b>\n\nВыберите тип данных для экспорта:"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Все заявки", callback_data="export_all")],
            [InlineKeyboardButton(text="✅ Только одобренные", callback_data="export_approved")],
            [InlineKeyboardButton(text="❌ Только отклоненные", callback_data="export_rejected")],
            [InlineKeyboardButton(text="⏳ Только ожидающие", callback_data="export_pending")]
        ])
        
        await callback_query.message.edit_text(export_message, reply_markup=keyboard)
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка при показе меню экспорта: {e}")
        await callback_query.answer("❌ Ошибка при загрузке меню", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('bulk_'))
async def bulk_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик массовых операций"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для массовых операций.", show_alert=True)
        return

    try:
        action = callback_query.data.split('_', 1)[1]
        
        await callback_query.answer("⚡ Выполняем массовую операцию...", show_alert=False)
        
        pending_apps = db_manager.get_pending_applications()
        processed_count = 0
        
        if action == "approve_students":
            for app in pending_apps:
                if app['form_name'] == 'Студент':
                    try:
                        public_message = format_public_message(app['data'])
                        await bot.send_message(CHANNEL_ID, public_message)
                        db_manager.update_application_status(app['id'], 'approved')
                        processed_count += 1
                        await asyncio.sleep(0.5)  # Небольшая задержка
                    except Exception as e:
                        logger.error(f"Ошибка публикации студента {app['id']}: {e}")
                        
        elif action == "approve_startups":
            for app in pending_apps:
                if app['form_name'] == 'Стартап':
                    try:
                        public_message = format_public_message(app['data'])
                        await bot.send_message(CHANNEL_ID, public_message)
                        db_manager.update_application_status(app['id'], 'approved')
                        processed_count += 1
                        await asyncio.sleep(0.5)  # Небольшая задержка
                    except Exception as e:
                        logger.error(f"Ошибка публикации стартапа {app['id']}: {e}")
                        
        elif action == "reject_old":
            cutoff_date = datetime.now() - timedelta(days=7)
            for app in pending_apps:
                app_date = datetime.fromisoformat(app['created_at'])
                if app_date < cutoff_date:
                    db_manager.update_application_status(app['id'], 'rejected')
                    processed_count += 1
        
        result_text = f"✅ Массовая операция завершена!\n\nОбработано заявок: {processed_count}"
        
        # Обновляем сообщение
        await callback_query.message.edit_text(
            callback_query.message.text + f"\n\n{result_text}",
            reply_markup=None
        )

    except Exception as e:
        logger.error(f"Ошибка массовой операции: {e}")
        await callback_query.answer("❌ Ошибка выполнения операции", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith('export_'))
async def export_callback_handler(callback_query: types.CallbackQuery):
    """Обработчик экспорта данных"""
    if callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("❌ У вас нет прав для экспорта.", show_alert=True)
        return

    try:
        export_type = callback_query.data.split('_')[1]
        
        if export_type == 'menu':
            return  # Уже обработано в export_menu_callback_handler
        
        await callback_query.answer("📄 Генерируем файл экспорта...", show_alert=False)
        
        # Определяем статус для экспорта
        status_map = {
            'all': None,
            'approved': 'approved',
            'rejected': 'rejected',
            'pending': 'pending'
        }
        
        status = status_map.get(export_type)
        filename = db_manager.export_to_csv(status)
        
        if filename:
            with open(filename, 'rb') as file:
                caption = f"📄 Экспорт заявок ({export_type})\n🕐 {datetime.now().strftime('%H:%M %d.%m.%Y')}"
                await bot.send_document(ADMIN_ID, file, caption=caption)
            os.remove(filename)  # Удаляем временный файл
            
            await callback_query.message.answer("✅ Файл экспорта отправлен!")
        else:
            await callback_query.answer("❌ Ошибка при создании файла экспорта", show_alert=True)

    except Exception as e:
        logger.error(f"Ошибка при экспорте ({export_type}): {e}")
        await callback_query.answer("❌ Ошибка при экспорте данных", show_alert=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    try:
        # Запускаем polling в отдельной задаче
        polling_task = asyncio.create_task(dp.start_polling(bot, skip_updates=True))
        logger.info("🚀 Бот успешно запущен")
        logger.info(f"📋 Admin ID: {ADMIN_ID}")
        logger.info(f"📢 Channel: {CHANNEL_ID}")
        
        # Автоматическая очистка старых заявок при запуске
        try:
            deleted_count = db_manager.cleanup_old_applications(30)
            if deleted_count > 0:
                logger.info(f"🗑️ При запуске удалено {deleted_count} старых заявок")
        except Exception as e:
            logger.warning(f"Ошибка автоочистки при запуске: {e}")
        
        # Отправляем уведомление админу о запуске
        try:
            stats = db_manager.get_statistics()
            startup_msg = (
                "🚀 <b>Бот запущен и готов к работе!</b>\n\n"
                "📊 Все системы в норме.\n"
                "💾 База данных подключена.\n\n"
                f"📈 <b>Текущая статистика:</b>\n"
                f"• В ожидании: {stats['pending']}\n"
                f"• Всего обработано: {stats['total_received']}"
            )
            await bot.send_message(ADMIN_ID, startup_msg)
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о запуске: {e}")
            
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    
    yield
    
    # Завершение
    logger.info("🛑 Завершение работы бота")
    try:
        polling_task.cancel()
        await bot.session.close()
    except Exception as e:
        logger.error(f"Ошибка при завершении: {e}")

app = FastAPI(title="Kickoff Bot API", version="2.0.0", lifespan=lifespan)

@app.post("/webhook")
async def webhook_handler(request: Request, background_tasks: BackgroundTasks):
    """Обработчик webhook от Tally"""
    try:
        # Получаем сырые данные
        raw_data = await request.body()
        logger.info(f"📥 Получен webhook (размер: {len(raw_data)} байт)")

        # Пытаемся распарсить JSON
        try:
            data = await request.json()
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            raise HTTPException(status_code=400, detail="Неверный формат JSON")

        # Валидируем данные
        if not validate_webhook_data(data):
            logger.warning("Данные webhook не прошли валидацию")
            raise HTTPException(status_code=400, detail="Неверная структура данных")

        # Генерируем ID для заявки
        app_id = generate_application_id()
        form_name = data.get('data', {}).get('formName', '')

        # Создаем резервную копию в фоне
        background_tasks.add_task(backup_application, app_id, data)

        # Сохраняем заявку в БД
        backup_filename = f"backup_{app_id}.json"
        db_manager.save_application(app_id, form_name, data, backup_filename)
        logger.info(f"💾 Заявка #{app_id} сохранена в БД (тип: {form_name})")

        # Формируем сообщение для админа
        admin_message = format_application_message(data, app_id)

        # Создаем inline-кнопки для быстрого одобрения/отклонения
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{app_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{app_id}")
            ],
            [InlineKeyboardButton(text="👀 Подробнее", callback_data=f"details_{app_id}")]
        ])

        # Отправляем админу в фоне
        background_tasks.add_task(send_notification_to_admin, admin_message, keyboard)

        logger.info(f"✅ Заявка #{app_id} успешно обработана")

        return {
            "status": "success", 
            "application_id": app_id,
            "form_type": form_name,
            "timestamp": datetime.now().isoformat(),
            "saved_to_database": True
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при обработке webhook: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@app.get("/")
async def root():
    """Главная страница с подробной информацией"""
    stats = db_manager.get_statistics()
    return {
        "service": "Kickoff Bot API",
        "version": "2.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "database": "connected",
        "statistics": stats,
        "endpoints": {
            "webhook": "/webhook",
            "health": "/health",
            "test_webhook": "/test-webhook",
            "metrics": "/metrics"
        }
    }

@app.get("/health")
async def health():
    """Расширенная проверка здоровья сервиса"""
    try:
        # Проверяем подключение к боту
        bot_info = await bot.get_me()
        
        # Проверяем БД
        stats = db_manager.get_statistics()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "bot": {
                "username": bot_info.username,
                "id": bot_info.id,
                "status": "connected"
            },
            "database": {
                "status": "connected",
                "total_applications": stats['total_received']
            },
            "statistics": stats
        }
    except Exception as e:
        logger.error(f"Ошибка проверки здоровья: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/metrics")
async def metrics():
    """Метрики для мониторинга"""
    try:
        stats = db_manager.get_statistics()
        
        # Дополнительные метрики
        pending_apps = db_manager.get_pending_applications()
        avg_response_time = "N/A"  # Можно добавить расчет среднего времени обработки
        
        return {
            "timestamp": datetime.now().isoformat(),
            "applications": {
                "total": stats['total_received'],
                "approved": stats['total_approved'],
                "rejected": stats['total_rejected'],
                "pending": stats['pending'],
                "approval_rate": round(stats['total_approved'] / max(stats['total_received'], 1) * 100, 2)
            },
            "forms": {
                "startups": stats['startup_forms'],
                "students": stats['student_forms']
            },
            "performance": {
                "average_response_time": avg_response_time,
                "oldest_pending": pending_apps[0]['created_at'] if pending_apps else None
            }
        }
    except Exception as e:
        logger.error(f"Ошибка получения метрик: {e}")
        return {"error": str(e)}

@app.post("/test-webhook")
async def test_webhook():
    """Тестовый webhook для проверки работы"""
    test_data = {
        "eventType": "FORM_RESPONSE",
        "data": {
            "formName": "Тест",
            "fields": [
                {"label": "Имя", "value": "Тестовый Пользователь", "type": "INPUT_TEXT"},
                {"label": "E-mail", "value": "test@example.com", "type": "INPUT_TEXT"},
                {"label": "Телефон", "value": "+7 999 123 45 67", "type": "INPUT_TEXT"},
                {"label": "Навыки", "value": "Python, JavaScript", "type": "INPUT_TEXT"},
                {"label": "Опыт", "value": "3 года", "type": "INPUT_TEXT"}
            ]
        }
    }

    try:
        # Генерируем ID для заявки
        app_id = generate_application_id()
        form_name = test_data.get('data', {}).get('formName', '')

        # Сохраняем в БД
        db_manager.save_application(app_id, form_name, test_data)

        # Формируем сообщение для админа
        admin_message = format_application_message(test_data, app_id)

        # Отправляем админу
        await bot.send_message(ADMIN_ID, admin_message)

        logger.info(f"🧪 Тестовая заявка #{app_id} отправлена админу")

        return {
            "status": "success", 
            "application_id": app_id, 
            "message": "Тестовая заявка успешно отправлена",
            "timestamp": datetime.now().isoformat(),
            "saved_to_database": True
        }

    except Exception as e:
        logger.error(f"Ошибка при отправке тестовой заявки: {e}")
        return {
            "status": "error", 
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    logger.info("🚀 Запуск Kickoff Bot API v2.0...")
    uvicorn.run(app, host="0.0.0.0", port=3000)
