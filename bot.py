import os
import ssl
import certifi
import logging
import sqlite3
import random
import re
from html import escape
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, ChatPermissions, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, filters, JobQueue
import requests
import aiohttp
from functools import lru_cache
from cryptography.fernet import Fernet
import hashlib
import time

# =========================================================
# CONFIGURACIÓN
# =========================================================

BOT_TOKEN = "8752924542:AAH9_zy4Sa2cE7AAqJi9bZHshYDF1Jh6ggo"
MAIN_GROUP_ID = -1003777076233
REFE_GROUP_ID = -1003566231864
LOG_GROUP_ID = -5154377611
SCAM_GROUP_ID = -1003810336549
SUPERADMIN_IDS = {8747380388}
AUTHORIZED_ADMIN_IDS = {5441572575, 8747380388}
DEFAULT_REFE_TEXT = (
    "✨ <b>¿Quieres resultados así?</b>\n\n"
    "<b>ÚNETE A LA ELITE</b>.\n"
    "🚀 Atención personalizada\n"
    "💎 Acceso exclusivo\n"
    "🔥 Staff autorizado"
)
MX_TZ = timezone(timedelta(hours=-6))
DB = "bot.db"
API_URL = "https://leviatan-chk.site/amazon/leviatan"

# Configuración SSL
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CERT_FILE"] = certifi.where()
ssl._create__https_context = ssl.create_default_context(cafile=certifi.where())

# Logging
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot_detailed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("vip_bot")

# =========================================================
# FUNCIONES DE GENERACIÓN DE TARJETAS (LUHN ALGORITHM)
# =========================================================

def calculate_luhn_check_digit(number):
    """Calcula el dígito de verificación de Luhn para un número dado."""
    total = 0
    reverse_digits = number[::-1]
    
    for i, digit in enumerate(reverse_digits):
        n = int(digit)
        
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        
        total += n
    
    check_digit = (10 - (total % 10)) % 10
    return check_digit

def generate_valid_card_from_pattern(pattern):
    """Genera una tarjeta válida a partir de un patrón con 'x'."""
    parts = pattern.split('|')
    if len(parts) != 4:
        raise ValueError("Formato inválido. Debe ser: numero|mes|año|cvv")
    
    number_pattern, month_pattern, year_pattern, cvv_pattern = parts
    
    # ===== GENERAR NÚMERO DE TARJETA =====
    generated_number = ""
    for char in number_pattern:
        if char == 'x' or char == 'X':
            generated_number += str(random.randint(0, 9))
        else:
            generated_number += char
    
    if len(generated_number) < 16:
        while len(generated_number) < 15:
            generated_number += str(random.randint(0, 9))
    
    first_15 = generated_number[:15]
    check_digit = calculate_luhn_check_digit(first_15)
    final_number = first_15 + str(check_digit)
    
    # ===== GENERAR FECHA DE EXPIRACIÓN =====
    month = ""
    for char in month_pattern:
        if char == 'x' or char == 'X':
            month += str(random.randint(0, 9))
        else:
            month += char
    
    if month == "xx" or month == "XX":
        month = str(random.randint(1, 12)).zfill(2)
    else:
        month_int = int(month) if month.isdigit() else random.randint(1, 12)
        month_int = max(1, min(12, month_int))
        month = str(month_int).zfill(2)
    
    # Año
    year = ""
    for char in year_pattern:
        if char == 'x' or char == 'X':
            year += str(random.randint(0, 9))
        else:
            year += char
    
    current_year = datetime.now().year
    if year == "xxxx" or year == "XXXX":
        year = str(current_year + random.randint(1, 5))
    else:
        if len(year) == 2:
            year_int = int(year) if year.isdigit() else random.randint(24, 29)
            year = str(2000 + year_int)
        elif len(year) == 4 and year.isdigit():
            year_int = int(year)
            if year_int < current_year:
                year_int = current_year + random.randint(1, 5)
                year = str(year_int)
    
    # ===== GENERAR CVV =====
    cvv = ""
    for char in cvv_pattern:
        if char == 'x' or char == 'X':
            cvv += str(random.randint(0, 9))
        else:
            cvv += char
    
    if cvv == "xxx" or cvv == "XXX":
        cvv = str(random.randint(100, 999))
    elif len(cvv) < 3:
        cvv = cvv.ljust(3, str(random.randint(0, 9)))
    
    return f"{final_number}|{month}|{year}|{cvv}"

def generate_multiple_cards(pattern, count=1):
    """Genera múltiples tarjetas válidas a partir de un patrón."""
    cards = []
    for _ in range(count):
        try:
            card = generate_valid_card_from_pattern(pattern)
            cards.append(card)
        except Exception as e:
            logger.error(f"Error generando tarjeta: {e}")
            default_card = generate_default_card()
            cards.append(default_card)
    return cards

def generate_default_card():
    """Genera una tarjeta por defecto si el patrón falla."""
    bin_prefix = "5428780"
    random_digits = ''.join(str(random.randint(0, 9)) for _ in range(8))
    first_15 = bin_prefix + random_digits
    check_digit = calculate_luhn_check_digit(first_15)
    card_number = first_15 + str(check_digit)
    month = str(random.randint(1, 12)).zfill(2)
    year = str(datetime.now().year + random.randint(1, 5))
    cvv = str(random.randint(100, 999))
    return f"{card_number}|{month}|{year}|{cvv}"

# =========================================================
# ENCRIPTACIÓN DE COOKIES
# =========================================================

def get_encryption_key() -> bytes:
    """Obtiene o genera una clave de encriptación."""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key='encryption_key'")
    row = c.fetchone()
    
    if row:
        key = row[0].encode()
    else:
        key = Fernet.generate_key()
        c.execute("INSERT INTO settings(key, value) VALUES('encryption_key', ?)", (key.decode(),))
        conn.commit()
    
    conn.close()
    return key

def encrypt_cookie(cookie: str) -> str:
    """Encripta una cookie."""
    if not cookie:
        return ""
    fernet = Fernet(get_encryption_key())
    encrypted = fernet.encrypt(cookie.encode())
    return encrypted.decode()

def decrypt_cookie(encrypted_cookie: str) -> str:
    """Desencripta una cookie."""
    if not encrypted_cookie:
        return ""
    try:
        fernet = Fernet(get_encryption_key())
        decrypted = fernet.decrypt(encrypted_cookie.encode())
        return decrypted.decode()
    except Exception as e:
        logger.error(f"Error al desencriptar cookie: {e}")
        return ""

# =========================================================
# RATE LIMITING
# =========================================================

def init_rate_limit_table():
    """Crea la tabla para rate limiting si no existe."""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS rate_limits(
        user_id INTEGER,
        command TEXT,
        last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, command)
    )
    """)
    conn.commit()
    conn.close()

def check_rate_limit(user_id: int, command: str, cooldown_seconds: int) -> bool:
    """Verifica si un usuario puede usar un comando (rate limiting)."""
    if user_id in AUTHORIZED_ADMIN_IDS or user_id in SUPERADMIN_IDS:
        return True  # Admins sin límites
    
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
    SELECT last_used FROM rate_limits 
    WHERE user_id=? AND command=?
    """, (user_id, command))
    
    row = c.fetchone()
    
    if not row:
        # Primera vez que usa el comando
        c.execute("""
        INSERT INTO rate_limits(user_id, command, last_used) 
        VALUES(?, ?, CURRENT_TIMESTAMP)
        """, (user_id, command))
        conn.commit()
        conn.close()
        return True
    
    last_used = datetime.fromisoformat(row[0])
    now = datetime.now()
    elapsed = (now - last_used).total_seconds()
    
    if elapsed >= cooldown_seconds:
        # Puede usar el comando, actualizar timestamp
        c.execute("""
        UPDATE rate_limits SET last_used=CURRENT_TIMESTAMP 
        WHERE user_id=? AND command=?
        """, (user_id, command))
        conn.commit()
        conn.close()
        return True
    
    conn.close()
    return False

def get_remaining_cooldown(user_id: int, command: str) -> int:
    """Obtiene los segundos restantes de cooldown."""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT last_used FROM rate_limits WHERE user_id=? AND command=?", (user_id, command))
    row = c.fetchone()
    conn.close()
    
    if not row:
        return 0
    
    last_used = datetime.fromisoformat(row[0])
    now = datetime.now()
    elapsed = (now - last_used).total_seconds()
    
    # Cooldowns específicos por comando
    cooldowns = {
        "mx": 30,      # 30 segundos para /mx
        "refe": 60,    # 60 segundos para /refe
        "cuki": 300,   # 5 minutos para /cuki
        "gen": 60,     # 60 segundos para /gen
    }
    
    cooldown = cooldowns.get(command, 30)
    remaining = max(0, cooldown - int(elapsed))
    return remaining

# =========================================================
# BASE DE DATOS
# =========================================================

def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_column_exists(table_name: str, column_name: str, column_sql: str):
    conn = db()
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    cols = [row["name"] for row in c.fetchall()]
    if column_name not in cols:
        c.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
        conn.commit()
    conn.close()

def init_db():
    conn = db()
    c = conn.cursor()
    
    # Tabla users
    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
        telegram_id INTEGER PRIMARY KEY,
        username TEXT,
        name TEXT,
        warns INTEGER DEFAULT 0,
        cookie TEXT,
        started_bot INTEGER DEFAULT 0,
        banned INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Tabla plans
    c.execute("""
    CREATE TABLE IF NOT EXISTS plans(
        telegram_id INTEGER PRIMARY KEY,
        end_date TEXT,
        last_day_warn_sent INTEGER DEFAULT 0
    )
    """)
    
    # Tabla settings
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    
    # Tabla refe_stats
    c.execute("""
    CREATE TABLE IF NOT EXISTS refe_stats(
        sender_id INTEGER PRIMARY KEY,
        count INTEGER DEFAULT 0
    )
    """)
    
    conn.commit()
    conn.close()
    
    # Columnas adicionales
    ensure_column_exists("users", "cookie", "TEXT")
    ensure_column_exists("plans", "last_day_warn_sent", "INTEGER DEFAULT 0")
    
    # Inicializar rate limiting
    init_rate_limit_table()
    
    # Configuración por defecto
    set_setting_if_missing("refe_text", DEFAULT_REFE_TEXT)
    
    # Generar clave de encriptación si no existe
    get_encryption_key()

def set_setting_if_missing(key: str, value: str):
    conn = db()
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = c.fetchone()
    if not row:
        c.execute("INSERT INTO settings(key, value) VALUES(?, ?)", (key, value))
        conn.commit()
    conn.close()

def get_setting(key: str, default: str = "") -> str:
    conn = db()
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    conn = db()
    c = conn.cursor()
    c.execute("""
    INSERT INTO settings(key, value) VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()
    conn.close()

# =========================================================
# FUNCIONES AUXILIARES
# =========================================================

def now_mx():
    return datetime.now(MX_TZ)

def is_admin(user_id: int) -> bool:
    return user_id in AUTHORIZED_ADMIN_IDS or user_id in SUPERADMIN_IDS

def get_user_row(user_id: int):
    conn = db()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE telegram_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_plan_row(user_id: int):
    conn = db()
    c = conn.cursor()
    c.execute("SELECT end_date FROM plans WHERE telegram_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def plan_active(user_id: int) -> bool:
    row = get_plan_row(user_id)
    if not row:
        return False
    end = datetime.fromisoformat(row["end_date"])
    return end > now_mx()

def plan_remaining(user_id: int) -> str:
    row = get_plan_row(user_id)
    if not row:
        return "Sin plan"
    end = datetime.fromisoformat(row["end_date"])
    diff = end - now_mx()
    if diff.total_seconds() <= 0:
        return "Expirado"
    return str(diff).split(".")[0]

def ensure_user_registered(user):
    conn = db()
    c = conn.cursor()
    c.execute("""
    INSERT OR IGNORE INTO users (telegram_id, username, name, created_at)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (user.id, user.username, user.full_name))
    conn.commit()
    conn.close()

def increment_refe_count(user_id: int):
    conn = db()
    c = conn.cursor()
    c.execute("""
    INSERT INTO refe_stats(sender_id, count) VALUES(?, 1)
    ON CONFLICT(sender_id) DO UPDATE SET count=count+1
    """, (user_id,))
    conn.commit()
    conn.close()

async def safe_reply(message, text: str, parse_mode: str = ParseMode.HTML):
    """Envía un mensaje con formato HTML por defecto."""
    try:
        await message.reply_text(text, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Error al enviar mensaje: {e}")
        # Fallback sin formato
        await message.reply_text(text.replace('<', '&lt;').replace('>', '&gt;'))

async def send_log(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not LOG_GROUP_ID:
        return
    try:
        await context.bot.send_message(LOG_GROUP_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning("No se pudo mandar log: %s", e)

def log_command(user_id: int, username: str, command: str, chat_id: int = None):
    """Registra el uso de un comando con detalles."""
    timestamp = now_mx().strftime("%Y-%m-%d %H:%M:%S")
    chat_info = f"chat:{chat_id}" if chat_id else "DM"
    log_msg = f"CMD | user:{user_id} (@{username}) | {command} | {chat_info} | {timestamp}"
    logger.info(log_msg)

# =========================================================
# VALIDACIÓN DE PLAN ACTIVO
# =========================================================

def require_active_plan(func):
    """Decorador para requerir plan activo en comandos."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        # Admins pueden usar todos los comandos
        if is_admin(user_id):
            return await func(update, context)
        
        # Verificar plan activo
        if not plan_active(user_id):
            await safe_reply(update.message, 
                "❌ <b>Error:</b> Necesitas un plan activo para usar este comando.\n\n"
                "Usa <code>/mi_plan</code> para verificar tu estado.\n"
                "Contacta al staff para adquirir un plan.")
            return
        
        # Si tiene plan activo, ejecutar el comando
        return await func(update, context)
    return wrapper

# =========================================================
# VERIFICACIÓN DE TARJETAS (MULTIPLE) CON RATE LIMITING
# =========================================================

@require_active_plan
async def mx_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    user = update.effective_user
    
    # Log del comando
    log_command(user_id, user.username or "sin_username", "/mx", msg.chat_id)
    
    # Rate limiting
    if not check_rate_limit(user_id, "mx", 30):
        remaining = get_remaining_cooldown(user_id, "mx")
        await safe_reply(msg, f"⏳ <b>Espera {remaining} segundos</b> antes de usar /mx nuevamente.")
        return
    
    user_row = get_user_row(user_id)

    if not user_row or not user_row["cookie"]:
        await safe_reply(msg,
            "❌ <b>Error:</b> Necesitas una cookie guardada.\n\n"
            "📝 <b>¿Cómo guardar tu cookie?</b>\n"
            "1. Responde al mensaje con tu cookie\n"
            "2. Usa el comando: <code>/cuki</code>\n\n"
            "💡 <b>Tip:</b> Guarda tu cookie una vez y el bot la recordará")
        return

    cookie = decrypt_cookie(user_row["cookie"])

    text = ""
    if context.args:
        text = " ".join(context.args)
    elif msg.text:
        text = msg.text

    if text.startswith('/mx'):
        text = text[3:].strip()

    if not text and msg.reply_to_message and msg.reply_to_message.text:
        text = msg.reply_to_message.text
    elif not text:
        await safe_reply(msg,
            "❌ <b>Uso:</b> Responde al mensaje con las tarjetas o escribe <code>/mx</code> seguido de las tarjetas.\n\n"
            "📝 <b>Formato válido:</b>\n"
            "<code>numero|mes|año|cvv</code>\n\n"
            "💡 <b>Ejemplo:</b>\n"
            "<code>5317223259757842|11|2033|030</code>")
        return

    lines = text.strip().split('\n')
    cards_to_check = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split('|')
        if len(parts) == 4:
            cards_to_check.append(f"{parts[0]}|{parts[1]}|{parts[2]}|{parts[3]}")
        else:
            parts = line.replace('|', ' ').split()
            if len(parts) == 4:
                cards_to_check.append(f"{parts[0]}|{parts[1]}|{parts[2]}|{parts[3]}")

    if not cards_to_check:
        await safe_reply(msg,
            "❌ <b>Error:</b> No se encontraron tarjetas válidas.\n\n"
            "📝 <b>Formato correcto:</b>\n"
            "<code>numero|mes|año|cvv</code>\n\n"
            "💡 <b>Ejemplo:</b>\n"
            "<code>5317223259757842|11|2033|030</code>\n\n"
            "📝 <b>Para generar tarjetas:</b>\n"
            "Usa el comando: <code>/gen 1234567xxxxxxxxx|xx|xxxx|xxx</code>")
        return

    await safe_reply(msg, 
        f"🔍 <b>Verificando {len(cards_to_check)} tarjeta(s)...</b>\n\n"
        "⏳ <b>Espera un momento...</b>\n\n"
        "📊 <b>Procesando tarjetas...</b>")

    results = []
    for card in cards_to_check:
        try:
            result = await verify_card(card, cookie)
            results.append(result)
        except Exception as e:
            logger.error(f"Error verificando tarjeta {card}: {e}")
            results.append({
                "status": "Error",
                "message": f"Error de API: {str(e)}",
                "card": card
            })

    approved_count = 0
    declined_count = 0
    error_count = 0

    output_text = "📊 <b>RESULTADOS DE VERIFICACIÓN</b>\n\n"

    for res in results:
        status = res["status"]
        message = res["message"]
        card = res["card"]

        if "✅ Approved" in status or "Approved" in status:
            approved_count += 1
            status_icon = "✅"
        elif "❌ Declined" in status or "Declined" in status:
            declined_count += 1
            status_icon = "❌"
        else:
            error_count += 1
            status_icon = "⚠️"

        output_text += f"{status_icon} <code>{card}</code>\n"
        output_text += f"<i>{escape(message)}</i>\n\n"

    output_text += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    output_text += f"✅ <b>Aprobadas:</b> {approved_count}\n"
    output_text += f"❌ <b>Declinadas:</b> {declined_count}\n"
    output_text += f"⚠️ <b>Errores:</b> {error_count}\n"
    output_text += f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    output_text += f"📊 <b>Total:</b> {len(results)}"

    await safe_reply(msg, output_text)

async def verify_card(card_data: str, cookie: str) -> dict:
    data = {
        "card": card_data,
        "cookies": cookie
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, json=data, headers=headers, timeout=30) as response:
            result = await response.json()
            return {
                "status": result.get("status", "Error desconocido"),
                "message": result.get("message", "Sin mensaje"),
                "card": card_data
            }

# =========================================================
# COMANDO /gen PARA GENERAR TARJETAS
# =========================================================

@require_active_plan
async def gen_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera tarjetas de crédito válidas a partir de un patrón."""
    msg = update.message
    user_id = update.effective_user.id
    user = update.effective_user
    
    # Log del comando
    log_command(user_id, user.username or "sin_username", "/gen", msg.chat_id)
    
    # Rate limiting
    if not check_rate_limit(user_id, "gen", 60):
        remaining = get_remaining_cooldown(user_id, "gen")
        await safe_reply(msg, 
            f"⏳ <b>Espera {remaining} segundos</b> antes de usar /gen nuevamente.")
        return
    
    # Verificar argumentos
    if not context.args:
        await safe_reply(msg,
            "❌ <b>Uso:</b> <code>/gen patrón [cantidad]</code>\n\n"
            "📝 <b>Ejemplos:</b>\n"
            "<code>/gen 1234567xxxxxxxxx|xx|xxxx|xxx</code>\n"
            "<code>/gen 5428780xxxxxxxx|xx|xxxx|xxx 5</code>\n"
            "<code>/gen 40001234xxxxxxxx|xx|xxxx|xxx 10</code>\n\n"
            "💡 <b>Notas:</b>\n"
            "• Los dígitos fijos se mantienen\n"
            "• Las 'x' se reemplazan por dígitos aleatorios\n"
            "• El último dígito se ajusta para ser válido (Luhn)\n"
            "• Las fechas son futuras\n"
            "• Límite: 20 tarjetas por comando")
        return
    
    pattern = context.args[0]
    
    # Validar patrón básico
    if '|' not in pattern or pattern.count('|') != 3:
        await safe_reply(msg,
            "❌ <b>Formato inválido</b>\n\n"
            "El patrón debe tener el formato: <code>numero|mes|año|cvv</code>\n"
            "Ejemplo: <code>1234567xxxxxxxxx|xx|xxxx|xxx</code>")
        return
    
    # Obtener cantidad (por defecto 1)
    count = 1
    if len(context.args) > 1:
        try:
            count = int(context.args[1])
            count = min(max(1, count), 20)  # Límite de 20 tarjetas
        except ValueError:
            await safe_reply(msg, "❌ Cantidad inválida. Debe ser un número.")
            return
    
    # Mostrar mensaje de procesamiento
    processing_msg = await msg.reply_text(
        f"🔢 <b>Generando {count} tarjeta(s)...</b>\n\n"
        f"📝 <b>Patrón:</b> <code>{pattern}</code>\n\n"
        "⏳ <b>Espera un momento...</b>",
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Generar tarjetas
        cards = generate_multiple_cards(pattern, count)
        
        # Formatear respuesta
        if count == 1:
            response = (
                f"✅ <b>TARJETA GENERADA</b>\n\n"
                f"<code>{cards[0]}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📝 <b>Patrón usado:</b> <code>{pattern}</code>\n"
                f"✅ <b>Válida (Luhn check passed)</b>\n"
                f"📅 <b>Fecha futura:</b> Sí\n"
                f"🔐 <b>CVV válido:</b> Sí"
            )
        else:
            response = f"✅ <b>{count} TARJETAS GENERADAS</b>\n\n"
            response += f"📝 <b>Patrón:</b> <code>{pattern}</code>\n\n"
            response += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            for i, card in enumerate(cards, 1):
                response += f"{i}. <code>{card}</code>\n"
            
            response += "\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            response += f"✅ <b>Todas válidas (Luhn check passed)</b>\n"
            response += f"📅 <b>Fechas futuras:</b> Sí\n"
            response += f"🔐 <b>CVV válidos:</b> Sí"
        
        await processing_msg.edit_text(response, parse_mode=ParseMode.HTML)
        
        # Log
        await send_log(context, 
            f"User {user_id} generó {count} tarjetas con patrón: {pattern[:50]}...")
        
    except Exception as e:
        logger.error(f"Error en /gen: {e}")
        await processing_msg.edit_text(
            "❌ <b>Error al generar tarjetas</b>\n\n"
            f"Error: {escape(str(e))}\n\n"
            "💡 <b>Asegúrate de que el patrón tenga el formato correcto:</b>\n"
            "<code>numero|mes|año|cvv</code>\n\n"
            "📝 <b>Ejemplo:</b>\n"
            "<code>1234567xxxxxxxxx|xx|xxxx|xxx</code>",
            parse_mode=ParseMode.HTML
        )

# =========================================================
# MANEJADORES DE COMANDOS (RESTO DEL CÓDIGO)
# =========================================================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user_registered(user)
    log_command(user.id, user.username or "sin_username", "/start", update.message.chat_id)
    await safe_reply(update.message, 
        f"👋 <b>Bienvenido, {escape(user.first_name)}!</b>\n\n"
        "🤖 <b>Bot de verificación de tarjetas</b>\n\n"
        "Usa <code>/help</code> para ver todos los comandos disponibles.\n\n"
        "💡 <b>Consejo:</b> Guarda tu cookie con <code>/cuki</code> para verificar tarjetas.")

async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_command(user.id, user.username or "sin_username", "/help", update.message.chat_id)
    text = (
        "🤖 <b>COMANDOS DISPONIBLES</b>\n\n"
        "<b>👤 Usuarios:</b>\n"
        "• <code>/start</code> - Iniciar bot\n"
        "• <code>/help</code> - Ver comandos\n"
        "• <code>/mi_plan</code> - Ver estado de tu plan\n"
        "• <code>/ck</code> - Ver cookie guardada\n"
        "• <code>/cuki</code> - Guardar cookie (responde al mensaje)\n"
        "• <code>/mx</code> - Verificar tarjetas\n"
        "• <code>/refe</code> - Enviar referencia\n"
        "• <code>/gen</code> - Generar tarjetas válidas\n"
        "• <code>/staff</code> - Ver staff\n"
        "• <code>/precios</code> - Ver precios\n"
        "• <code>/id</code> - Ver tu ID\n\n"
        "<b>👑 Admins:</b>\n"
        "• <code>/panel</code> - Panel de control\n"
        "• <code>/plan ID dias</code> - Dar plan\n"
        "• <code>/ban ID</code> - Banear\n"
        "• <code>/unban ID</code> - Desbanear\n"
        "• <code>/warn ID [motivo]</code> - Advertir\n"
        "• <code>/unwarn ID</code> - Quitar advertencia\n"
        "• <code>/users [página]</code> - Listar usuarios\n"
        "• <code>/broadcast mensaje</code> - Enviar a todos\n"
        "• <code>/stats</code> - Estadísticas"
    )
    await safe_reply(update.message, text)

async def mi_plan_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    log_command(user_id, user.username or "sin_username", "/mi_plan", update.message.chat_id)
    
    status = "✅ <b>Activo</b>" if plan_active(user_id) else "❌ <b>Inactivo</b>"
    remaining = plan_remaining(user_id)
    
    if plan_active(user_id):
        await safe_reply(update.message, 
            f"📅 <b>ESTADO DEL PLAN</b>\n\n"
            f"👤 <b>Usuario:</b> {escape(user.first_name)}\n"
            f"📊 <b>Estado:</b> {status}\n"
            f"⏰ <b>Restante:</b> <code>{remaining}</code>\n\n"
            "💡 <b>Tip:</b> Tu plan expirará automáticamente después de la fecha indicada.")
    else:
        await safe_reply(update.message,
            f"📅 <b>ESTADO DEL PLAN</b>\n\n"
            f"👤 <b>Usuario:</b> {escape(user.first_name)}\n"
            f"📊 <b>Estado:</b> {status}\n\n"
            "❌ <b>No tienes un plan activo.</b>\n\n"
            "💡 <b>¿Qué hacer?</b>\n"
            "• Contacta al staff para adquirir un plan\n"
            "• Usa <code>/precios</code> para ver los precios\n\n"
            "🔗 <i>Enlaces de contacto:</i>\n"
            "• <code>@TheVax1</code>\n"
            "• <code>@ElcazaJR1</code>")

async def ck_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    log_command(user_id, user.username or "sin_username", "/ck", update.message.chat_id)
    
    row = get_user_row(user_id)
    cookie = decrypt_cookie(row["cookie"]) if row and row["cookie"] else ""
    
    if cookie:
        await safe_reply(update.message,
            f"🍪 <b>TU COOKIE</b>\n\n"
            f"<b>Usuario:</b> {escape(user.first_name)}\n"
            f"<b>ID:</b> <code>{user_id}</code>\n\n"
            f"<code>{escape(cookie)}</code>\n\n"
            "⚠️ <b>IMPORTANTE:</b>\n"
            "• No compartas tu cookie con nadie\n"
            "• Si la pierdes, usa <code>/cuki</code> para guardarla nuevamente\n"
            "• La cookie está encriptada en nuestra base de datos")
    else:
        await safe_reply(update.message,
            "❌ <b>No tienes cookies guardadas</b>\n\n"
            "📝 <b>¿Cómo guardar tu cookie?</b>\n\n"
            "1. Copia tu cookie (responde al mensaje con ella)\n"
            "2. Usa el comando: <code>/cuki</code>\n\n"
            "💡 <b>Tip:</b> Puedes guardar tu cookie una vez y el bot la recordará")

async def cuki_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    user = update.effective_user
    log_command(user_id, user.username or "sin_username", "/cuki", msg.chat_id)
    
    # Rate limiting
    if not check_rate_limit(user_id, "cuki", 300):
        remaining = get_remaining_cooldown(user_id, "cuki")
        await safe_reply(msg, 
            f"⏳ <b>Espera {remaining} segundos</b> antes de cambiar la cookie nuevamente.")
        return
    
    text = None
    
    if msg.reply_to_message:
        text = msg.reply_to_message.text
    elif msg.text:
        parts = msg.text.split(' ', 1)
        if len(parts) > 1:
            text = parts[1]
    
    if text:
        # Validar que la cookie tenga contenido
        if len(text.strip()) < 10:
            await safe_reply(msg, 
                "❌ <b>Cookie muy corta</b>\n\n"
                "La cookie debe tener contenido válido.\n"
                "Por favor, responde con la cookie completa.")
            return
        
        encrypted_cookie = encrypt_cookie(text)
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET cookie=? WHERE telegram_id=?", (encrypted_cookie, user_id))
        conn.commit()
        conn.close()
        
        await safe_reply(msg,
            f"✅ <b>Cookie guardada correctamente</b>\n\n"
            f"🔐 <b>Estado:</b> Encriptada y segura\n"
            f"📝 <b>Usuario:</b> {escape(user.first_name)}\n\n"
            "💡 <b>Tip:</b> Ahora puedes usar el comando <code>/mx</code> para verificar tarjetas")
    else:
        await safe_reply(msg,
            "❌ <b>Error al guardar cookie</b>\n\n"
            "📝 <b>¿Cómo guardar tu cookie?</b>\n\n"
            "1. Responde al mensaje con tu cookie\n"
            "2. O escribe: <code>/cuki TU_COOKIE</code>\n\n"
            "🔐 <b>Nota:</b> La cookie está encriptada y es segura")

async def refe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    user = update.effective_user
    log_command(user_id, user.username or "sin_username", "/refe", msg.chat_id)
    
    # Rate limiting
    if not check_rate_limit(user_id, "refe", 60):
        remaining = get_remaining_cooldown(user_id, "refe")
        await safe_reply(msg, f"⏳ <b>Espera {remaining} segundos</b> antes de enviar otra referencia.")
        return
    
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await safe_reply(msg, "❌ <b>Error:</b> Responde a una imagen para enviar una referencia.")
        return

    increment_refe_count(user_id)

    conn = db()
    c = conn.cursor()
    c.execute("SELECT count FROM refe_stats WHERE sender_id=?", (user_id,))
    row = c.fetchone()
    ref_count = row["count"] if row else 1
    conn.close()

    refe_text = get_setting("refe_text", DEFAULT_REFE_TEXT)

    formatted_refe = (
        f"🔥 <b>REFENENCIA ENVIADA</b>\n\n"
        f"👤 <b>USER:</b> {escape(user.first_name)} "
        f"{'@' + escape(user.username) if user.username else 'Sin username'}\n"
        f"📊 <b>Referencias Totales:</b> <code>{ref_count}</code>\n\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"{refe_text}\n\n"
        f"🔗 <i>CONTACTA AL STAFF</i>"
    )

    try:
        await context.bot.send_photo(
            chat_id=REFE_GROUP_ID,
            photo=msg.reply_to_message.photo[-1].file_id,
            caption=formatted_refe,
            parse_mode=ParseMode.HTML
        )

        await safe_reply(msg, 
            "✅ <b>Referencia enviada correctamente.</b>\n\n"
            f"📊 <b>Tu número de referencia:</b> <code>{ref_count}</code>")
    except Exception as e:
        logger.error(f"Error al enviar referencia: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo enviar la referencia. Intenta nuevamente.")

async def staff_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_command(user.id, user.username or "sin_username", "/staff", update.message.chat_id)
    
    text = (
        "👑 <b>STAFF OFICIAL</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>Administrador Principal</b>\n"
        "• <code>@TheVax1</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>Soporte</b>\n"
        "• <code>@ElcazaJR1</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📝 <b>Horario de atención:</b>\n"
        "• Lunes a Domingo: 9:00 AM - 11:00 PM (MX)\n\n"
        "💡 <b>¿Problemas?</b>\n"
        "• Contacta al staff directamente\n"
        "• Envía captura de pantalla del error\n"
        "• Sé paciente, responderemos lo antes posible"
    )
    await safe_reply(update.message, text)

async def precios_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_command(user.id, user.username or "sin_username", "/precios", update.message.chat_id)
    
    text = (
        "💎 <b>PRECIOS DE PLANES</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📅 <b>7 DÍAS</b>\n"
        "💰 <b>Costo:</b> <code>$200 MXN</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📅 <b>15 DÍAS</b>\n"
        "💰 <b>Costo:</b> <code>$350 MXN</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📅 <b>30 DÍAS</b>\n"
        "💰 <b>Costo:</b> <code>$500 MXN</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📝 <b>¿Cómo comprar?</b>\n"
        "1. Contacta al staff\n"
        "2. Realiza el pago\n"
        "3. El plan se activa automáticamente\n\n"
        "💡 <b>Tip:</b> Los planes más largos tienen mejor precio por día"
    )
    await safe_reply(update.message, text)

async def id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    log_command(user_id, user.username or "sin_username", "/id", update.message.chat_id)
    chat_id = update.effective_chat.id
    
    text = (
        "🆔 <b>TU INFORMACIÓN</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>ID de Telegram:</b>\n"
        f"<code>{user_id}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>Nombre:</b>\n"
        f"<code>{escape(user.first_name)}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>Username:</b>\n"
        f"<code>{user.username or 'Sin username'}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🏢 <b>ID del Grupo/Chat:</b>\n"
        f"<code>{chat_id}</code>"
    )
    await safe_reply(update.message, text)

async def idgr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_command(user.id, user.username or "sin_username", "/idgr", update.message.chat_id)
    chat_id = update.effective_chat.id
    await safe_reply(update.message, f"🏢 <b>ID DEL GRUPO:</b> <code>{chat_id}</code>")

# =========================================================
# NUEVOS COMANDOS: /users CON PAGINACIÓN
# =========================================================

async def users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ No autorizado.")
        return
    
    log_command(user.id, user.username or "sin_username", "/users", msg.chat_id)
    
    # Obtener página (por defecto 1)
    page = 1
    if context.args:
        try:
            page = int(context.args[0])
            if page < 1:
                page = 1
        except ValueError:
            page = 1
    
    # Calcular offset
    users_per_page = 10
    offset = (page - 1) * users_per_page
    
    # Obtener usuarios
    conn = db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total FROM users")
    total_users = c.fetchone()["total"]
    
    c.execute("""
    SELECT u.telegram_id, u.username, u.name, u.warns, u.banned, 
           p.end_date, 
           CASE WHEN p.end_date IS NOT NULL AND datetime(p.end_date) > datetime('now') 
                THEN 1 ELSE 0 END as has_active_plan
    FROM users u
    LEFT JOIN plans p ON u.telegram_id = p.telegram_id
    ORDER BY u.created_at DESC
    LIMIT ? OFFSET ?
    """, (users_per_page, offset))
    
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await safe_reply(msg, "📭 No hay usuarios registrados.")
        return
    
    # Calcular total de páginas
    total_pages = (total_users + users_per_page - 1) // users_per_page
    
    # Construir mensaje
    text = f"👥 <b>USUARIOS</b> (Página {page}/{total_pages})\n\n"
    
    for i, row in enumerate(rows, start=offset + 1):
        user_id = row["telegram_id"]
        username = f"@{row['username']}" if row["username"] else "Sin username"
        name = escape(row["name"] or "Sin nombre")
        warns = row["warns"]
        banned = "🔴" if row["banned"] else "🟢"
        plan = "✅" if row["has_active_plan"] else "❌"
        
        text += f"{i}. <code>{user_id}</code> | {username}\n"
        text += f"   📛 {name} | ⚠️{warns} | {banned} | Plan: {plan}\n\n"
    
    text += f"📊 <b>Total:</b> {total_users} usuarios"
    
    # Crear botones de paginación
    keyboard = []
    if page > 1:
        keyboard.append(InlineKeyboardButton("◀️ Anterior", callback_data=f"users_{page-1}"))
    if page < total_pages:
        keyboard.append(InlineKeyboardButton("▶️ Siguiente", callback_data=f"users_{page+1}"))
    
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    
    await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not query.data.startswith("users_"):
        return
    
    page = int(query.data.split("_")[1])
    
    # Reutilizar la lógica de users_handler
    users_per_page = 10
    offset = (page - 1) * users_per_page
    
    conn = db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total FROM users")
    total_users = c.fetchone()["total"]
    
    c.execute("""
    SELECT u.telegram_id, u.username, u.name, u.warns, u.banned, 
           p.end_date, 
           CASE WHEN p.end_date IS NOT NULL AND datetime(p.end_date) > datetime('now') 
                THEN 1 ELSE 0 END as has_active_plan
    FROM users u
    LEFT JOIN plans p ON u.telegram_id = p.telegram_id
    ORDER BY u.created_at DESC
    LIMIT ? OFFSET ?
    """, (users_per_page, offset))
    
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await query.edit_message_text("📭 No hay usuarios registrados.")
        return
    
    total_pages = (total_users + users_per_page - 1) // users_per_page
    
    text = f"👥 <b>USUARIOS</b> (Página {page}/{total_pages})\n\n"
    
    for i, row in enumerate(rows, start=offset + 1):
        user_id = row["telegram_id"]
        username = f"@{row['username']}" if row["username"] else "Sin username"
        name = escape(row["name"] or "Sin nombre")
        warns = row["warns"]
        banned = "🔴" if row["banned"] else "🟢"
        plan = "✅" if row["has_active_plan"] else "❌"
        
        text += f"{i}. <code>{user_id}</code> | {username}\n"
        text += f"   📛 {name} | ⚠️{warns} | {banned} | Plan: {plan}\n\n"
    
    text += f"📊 <b>Total:</b> {total_users} usuarios"
    
    keyboard = []
    if page > 1:
        keyboard.append(InlineKeyboardButton("◀️ Anterior", callback_data=f"users_{page-1}"))
    if page < total_pages:
        keyboard.append(InlineKeyboardButton("▶️ Siguiente", callback_data=f"users_{page+1}"))
    
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

# =========================================================
# NUEVOS COMANDOS: /unban y /unwarn
# =========================================================

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    args = context.args
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ No autorizado.")
        return
    
    log_command(user.id, user.username or "sin_username", "/unban", msg.chat_id)
    
    if not args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/unban ID</code>")
        return
    
    try:
        target_id = int(args[0])
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET banned=0 WHERE telegram_id=?", (target_id,))
        conn.commit()
        conn.close()
        
        await safe_reply(msg, f"✅ <b>Usuario {target_id} desbaneado.</b>")
        await send_log(context, f"Admin {user.id} desbaneó a {target_id}")
        
        # Notificar al usuario
        try:
            await context.bot.send_message(
                target_id,
                "✅ <b>Has sido desbaneado del sistema.</b>\n\n"
                "Ahora puedes volver a usar los comandos del bot."
            )
        except:
            pass  # No se pudo enviar DM
        
    except Exception as e:
        logger.error(f"Error al desbanear usuario: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo desbanear al usuario.")

async def unwarn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    args = context.args
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ No autorizado.")
        return
    
    log_command(user.id, user.username or "sin_username", "/unwarn", msg.chat_id)
    
    if not args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/unwarn ID</code>")
        return
    
    try:
        target_id = int(args[0])
        conn = db()
        c = conn.cursor()
        
        # Obtener warns actuales
        c.execute("SELECT warns FROM users WHERE telegram_id=?", (target_id,))
        row = c.fetchone()
        
        if not row:
            await safe_reply(msg, f"❌ Usuario {target_id} no encontrado.")
            return
        
        current_warns = row["warns"]
        new_warns = max(0, current_warns - 1)
        
        c.execute("UPDATE users SET warns=? WHERE telegram_id=?", (new_warns, target_id))
        conn.commit()
        conn.close()
        
        removed = current_warns - new_warns
        await safe_reply(msg, f"✅ <b>Removida {removed} advertencia a {target_id}.</b>\nNuevo total: {new_warns}")
        await send_log(context, f"Admin {user.id} removió advertencia a {target_id} (de {current_warns} a {new_warns})")
        
    except Exception as e:
        logger.error(f"Error al remover advertencia: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo remover la advertencia.")

# =========================================================
# NUEVO COMANDO: /broadcast
# =========================================================

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ No autorizado.")
        return
    
    log_command(user.id, user.username or "sin_username", "/broadcast", msg.chat_id)
    
    if not context.args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/broadcast mensaje</code>")
        return
    
    broadcast_text = " ".join(context.args)
    
    # Confirmación
    confirm_keyboard = [
        [
            InlineKeyboardButton("✅ Sí, enviar", callback_data="broadcast_confirm"),
            InlineKeyboardButton("❌ Cancelar", callback_data="broadcast_cancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(confirm_keyboard)
    
    await msg.reply_text(
        f"📢 <b>CONFIRMAR BROADCAST</b>\n\n"
        f"<b>Mensaje:</b>\n{broadcast_text}\n\n"
        f"<b>¿Estás seguro de enviar este mensaje a todos los usuarios?</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "broadcast_cancel":
        await query.edit_message_text("❌ <b>Broadcast cancelado.</b>", parse_mode=ParseMode.HTML)
        return
    
    if query.data == "broadcast_confirm":
        # Obtener todos los usuarios
        conn = db()
        c = conn.cursor()
        c.execute("SELECT telegram_id FROM users")
        rows = c.fetchall()
        conn.close()
        
        total_users = len(rows)
        successful = 0
        failed = 0
        
        await query.edit_message_text(
            f"📤 <b>Enviando broadcast...</b>\n\n"
            f"Progreso: 0/{total_users}\n"
            f"✅ Exitosos: 0\n"
            f"❌ Fallidos: 0",
            parse_mode=ParseMode.HTML
        )
        
        broadcast_text = query.message.text.split("Mensaje:")[1].split("¿Estás seguro")[0].strip()
        
        for i, row in enumerate(rows, 1):
            user_id = row["telegram_id"]
            
            try:
                await context.bot.send_message(user_id, broadcast_text, parse_mode=ParseMode.HTML)
                successful += 1
            except Exception as e:
                failed += 1
                logger.warning(f"Error enviando broadcast a {user_id}: {e}")
            
            # Actualizar progreso cada 10 usuarios
            if i % 10 == 0 or i == total_users:
                try:
                    await query.edit_message_text(
                        f"📤 <b>Enviando broadcast...</b>\n\n"
                        f"Progreso: {i}/{total_users}\n"
                        f"✅ Exitosos: {successful}\n"
                        f"❌ Fallidos: {failed}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
        
        await query.edit_message_text(
            f"✅ <b>BROADCAST COMPLETADO</b>\n\n"
            f"📊 <b>Estadísticas:</b>\n"
            f"• Total usuarios: {total_users}\n"
            f"• ✅ Exitosos: {successful}\n"
            f"• ❌ Fallidos: {failed}\n"
            f"• 📈 Tasa de éxito: {(successful/total_users*100):.1f}%",
            parse_mode=ParseMode.HTML
        )
        
        await send_log(
            context,
            f"📢 Admin {query.from_user.id} envió broadcast a {total_users} usuarios\n"
            f"✅ Exitosos: {successful} | ❌ Fallidos: {failed}"
        )

# =========================================================
# NUEVO COMANDO: /stats
# =========================================================

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ No autorizado.")
        return
    
    log_command(user.id, user.username or "sin_username", "/stats", msg.chat_id)
    
    conn = db()
    c = conn.cursor()
    
    # Total usuarios
    c.execute("SELECT COUNT(*) as total FROM users")
    total_users = c.fetchone()["total"]
    
    # Usuarios con plan activo
    c.execute("""
    SELECT COUNT(*) as active FROM plans 
    WHERE datetime(end_date) > datetime('now')
    """)
    active_plans = c.fetchone()["active"]
    
    # Usuarios baneados
    c.execute("SELECT COUNT(*) as banned FROM users WHERE banned=1")
    banned_users = c.fetchone()["banned"]
    
    # Total warns
    c.execute("SELECT SUM(warns) as total_warns FROM users")
    total_warns = c.fetchone()["total_warns"] or 0
    
    # Total referencias
    c.execute("SELECT SUM(count) as total_refs FROM refe_stats")
    total_refs = c.fetchone()["total_refs"] or 0
    
    # Usuarios con cookie
    c.execute("SELECT COUNT(*) as with_cookie FROM users WHERE cookie IS NOT NULL AND cookie != ''")
    with_cookie = c.fetchone()["with_cookie"]
    
    # Usuarios que iniciaron el bot
    c.execute("SELECT COUNT(*) as started FROM users WHERE started_bot=1")
    started_bot = c.fetchone()["started"]
    
    conn.close()
    
    text = (
        "📊 <b>ESTADÍSTICAS GLOBALES</b>\n\n"
        f"👥 <b>Total usuarios:</b> {total_users}\n"
        f"✅ <b>Planes activos:</b> {active_plans}\n"
        f"🔴 <b>Usuarios baneados:</b> {banned_users}\n"
        f"⚠️ <b>Total advertencias:</b> {total_warns}\n"
        f"🔥 <b>Referencias enviadas:</b> {total_refs}\n"
        f"🍪 <b>Con cookie guardada:</b> {with_cookie}\n"
        f"🤖 <b>Iniciaron el bot:</b> {started_bot}\n\n"
        f"📈 <b>Porcentaje con plan activo:</b> {(active_plans/total_users*100 if total_users > 0 else 0):.1f}%"
    )
    
    await safe_reply(msg, text)

# =========================================================
# PANEL ADMIN (BÁSICO)
# =========================================================

async def panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_reply(update.message, "❌ No autorizado.")
        return
    
    user = update.effective_user
    log_command(user.id, user.username or "sin_username", "/panel", update.message.chat_id)
    
    text = (
        "🎛 <b>PANEL DE ADMINISTRACIÓN</b>\n\n"
        "<b>Comandos disponibles:</b>\n"
        "• <code>/users [página]</code> - Listar usuarios\n"
        "• <code>/plan ID dias</code> - Asignar plan\n"
        "• <code>/ban ID</code> - Banear usuario\n"
        "• <code>/unban ID</code> - Desbanear usuario\n"
        "• <code>/warn ID [motivo]</code> - Advertir usuario\n"
        "• <code>/unwarn ID</code> - Remover advertencia\n"
        "• <code>/broadcast mensaje</code> - Enviar a todos\n"
        "• <code>/stats</code> - Ver estadísticas\n\n"
        "<b>Configuración:</b>\n"
        "• <code>/set_refe_text</code> - Cambiar texto de referencia\n"
    )
    await safe_reply(update.message, text)

async def plan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    args = context.args
    
    if not user or not msg or not args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/plan ID dias</code>")
        return
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ <b>No autorizado.</b>")
        return
    
    log_command(user.id, user.username or "sin_username", "/plan", msg.chat_id)
    
    try:
        target_id = int(args[0])
        days = int(args[1])
        
        if days <= 0:
            await safe_reply(msg, "❌ Los días deben ser mayores a 0.")
            return
        
        end_date = now_mx() + timedelta(days=days)
        
        conn = db()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO plans (telegram_id, end_date, last_day_warn_sent) VALUES (?, ?, 0)", 
                 (target_id, end_date.isoformat()))
        conn.commit()
        conn.close()
        
        await safe_reply(msg, f"✅ <b>Plan asignado:</b> <code>{days} días</code> a <code>{target_id}</code>\nFecha de expiración: {end_date.strftime('%Y-%m-%d %H:%M')}")
        await send_log(context, f"Admin {user.id} asignó {days} días de plan a {target_id}")
        
        # Notificar al usuario
        try:
            await context.bot.send_message(
                target_id,
                f"🎉 <b>¡SE TE HA ASIGNADO UN PLAN!</b>\n\n"
                f"📅 <b>Duración:</b> {days} días\n"
                f"⏰ <b>Expira:</b> {end_date.strftime('%Y-%m-%d %H:%M')}\n\n"
                f"Ahora puedes usar todos los comandos del bot."
            )
        except Exception as e:
            logger.warning(f"No se pudo notificar al usuario {target_id}: {e}")
        
    except Exception as e:
        logger.error(f"Error al asignar plan: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo asignar el plan. Revisa los logs para más detalles.")

async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    args = context.args
    
    if not user or not msg or not args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/ban ID</code>")
        return
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ <b>No autorizado.</b>")
        return
    
    log_command(user.id, user.username or "sin_username", "/ban", msg.chat_id)
    
    try:
        target_id = int(args[0])
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET banned=1 WHERE telegram_id=?", (target_id,))
        conn.commit()
        conn.close()
        
        await safe_reply(msg, f"✅ <b>Usuario {target_id} baneado.</b>")
        await send_log(context, f"Admin {user.id} baneó a {target_id}")
        
        # Notificar al usuario
        try:
            await context.bot.send_message(
                target_id,
                "🚫 <b>HAS SIDO BANEADO DEL SISTEMA</b>\n\n"
                "Ya no podrás usar los comandos del bot.\n"
                "Contacta al staff si crees que es un error."
            )
        except:
            pass
        
    except Exception as e:
        logger.error(f"Error al banear usuario: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo banear al usuario. Revisa los logs para más detalles.")

async def warn_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    args = context.args
    
    if not user or not msg or not args:
        await safe_reply(msg, "❌ <b>Uso:</b> <code>/warn ID [motivo]</code>")
        return
    
    if not is_admin(user.id):
        await safe_reply(msg, "❌ <b>No autorizado.</b>")
        return
    
    log_command(user.id, user.username or "sin_username", "/warn", msg.chat_id)
    
    try:
        target_id = int(args[0])
        reason = ' '.join(args[1:]) if len(args) > 1 else "Sin motivo"
        
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET warns=warns+1 WHERE telegram_id=?", (target_id,))
        conn.commit()
        conn.close()
        
        await safe_reply(msg, f"✅ <b>Advertencia a {target_id}:</b> {reason}")
        await send_log(context, f"Admin {user.id} advirtió a {target_id} por: {reason}")
        
        # Notificar al usuario
        try:
            await context.bot.send_message(
                target_id,
                f"⚠️ <b>HAS RECIBIDO UNA ADVERTENCIA</b>\n\n"
                f"<b>Motivo:</b> {reason}\n\n"
                f"Si acumulas muchas advertencias podrías ser baneado."
            )
        except Exception as e:
            logger.warning(f"No se pudo notificar al usuario {target_id}: {e}")
        
    except Exception as e:
        logger.error(f"Error al advertir usuario: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo advertir al usuario. Revisa los logs para más detalles.")

# =========================================================
# VALIDACIÓN AL ENTRAR AL GRUPO
# =========================================================

async def new_chat_members_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja cuando nuevos usuarios entran al grupo."""
    if update.effective_chat.id != MAIN_GROUP_ID:
        return
    
    for new_member in update.message.new_chat_members:
        user_id = new_member.id
        
        # Ignorar si el bot mismo es agregado
        if user_id == context.bot.id:
            continue
        
        # Verificar si tiene plan activo
        if not plan_active(user_id):
            try:
                # Banear del grupo
                await context.bot.ban_chat_member(
                    chat_id=MAIN_GROUP_ID,
                    user_id=user_id
                )
                
                # Enviar mensaje en el grupo
                await update.message.reply_text(
                    f"🚫 <b>{escape(new_member.first_name)} ha sido expulsado</b>\n\n"
                    f"Razón: No tiene un plan activo.\n"
                    f"ID: <code>{user_id}</code>",
                    parse_mode=ParseMode.HTML
                )
                
                # Log
                logger.info(f"Usuario {user_id} expulsado del grupo por falta de plan activo")
                await send_log(
                    context,
                    f"🚫 Usuario {user_id} (@{new_member.username or 'sin_username'}) "
                    f"expulsado del grupo por falta de plan activo"
                )
                
            except Exception as e:
                logger.error(f"Error al expulsar usuario {user_id}: {e}")
        else:
            # Tiene plan activo, dar la bienvenida
            try:
                await update.message.reply_text(
                    f"👋 ¡Bienvenido {escape(new_member.first_name)}!\n\n"
                    f"Tu plan está activo hasta: {plan_remaining(user_id)}",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass

# =========================================================
# AUTOMATIZACIÓN (JobQueue)
# =========================================================

async def check_plans(context: ContextTypes.DEFAULT_TYPE):
    now = now_mx()
    conn = db()
    c = conn.cursor()
    c.execute("SELECT telegram_id, end_date, last_day_warn_sent FROM plans")
    rows = c.fetchall()
    
    for row in rows:
        user_id = row['telegram_id']
        end_date = datetime.fromisoformat(row['end_date'])
        last_day_warn_sent = row['last_day_warn_sent']
        
        # Verificar si expira mañana
        if (end_date.date() - now.date()).days == 1 and not last_day_warn_sent:
            try:
                await context.bot.send_message(
                    user_id,
                    "⚠️ <b>TU PLAN EXPIRA MAÑANA</b>\n\n"
                    "Renueva tu plan para evitar la expulsión del grupo."
                )
                c.execute("UPDATE plans SET last_day_warn_sent=1 WHERE telegram_id=?", (user_id,))
                conn.commit()
            except Exception as e:
                logger.warning(f"Error al enviar aviso de expiración a {user_id}: {e}")
        
        # Verificar si ya expiró
        elif end_date < now:
            try:
                # Expulsar del grupo principal
                await context.bot.ban_chat_member(chat_id=MAIN_GROUP_ID, user_id=user_id)
                
                # Eliminar plan
                c.execute("DELETE FROM plans WHERE telegram_id=?", (user_id,))
                conn.commit()
                
                # Notificar al usuario
                try:
                    await context.bot.send_message(
                        user_id,
                        "🚫 <b>TU PLAN HA EXPIRADO</b>\n\n"
                        "Has sido expulsado del grupo principal.\n"
                        "Renueva tu plan para volver a unirte."
                    )
                except:
                    pass
                
                logger.info(f"Usuario {user_id} expulsado por plan expirado")
                
            except Exception as e:
                logger.warning(f"Error al expulsar a {user_id}: {e}")
    
    conn.close()

# =========================================================
# INICIALIZACIÓN
# =========================================================

def main():
    # Inicializar base de datos
    init_db()
    
    # Crear aplicación
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Handlers de Usuarios
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CommandHandler("mi_plan", mi_plan_handler))
    application.add_handler(CommandHandler("ck", ck_handler))
    application.add_handler(CommandHandler("cuki", cuki_handler))
    application.add_handler(CommandHandler("mx", mx_handler))
    application.add_handler(CommandHandler("gen", gen_handler))  # NUEVO
    application.add_handler(CommandHandler("refe", refe_handler))
    application.add_handler(CommandHandler("staff", staff_handler))
    application.add_handler(CommandHandler("precios", precios_handler))
    application.add_handler(CommandHandler("id", id_handler))
    application.add_handler(CommandHandler("idgr", idgr_handler))
    
    # Nuevos comandos
    application.add_handler(CommandHandler("users", users_handler))
    application.add_handler(CommandHandler("unban", unban_cmd))
    application.add_handler(CommandHandler("unwarn", unwarn_cmd))
    application.add_handler(CommandHandler("broadcast", broadcast_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    
    # Handlers de Admin
    application.add_handler(CommandHandler("panel", panel_cmd))
    application.add_handler(CommandHandler("plan", plan_cmd))
    application.add_handler(CommandHandler("ban", ban_cmd))
    application.add_handler(CommandHandler("warn", warn_cmd))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(users_callback, pattern="^users_"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^broadcast_"))
    
    # Handler para nuevos miembros del grupo
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_members_handler))
    
    # JobQueue para tareas periódicas
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(check_plans, interval=timedelta(minutes=1), first=0)
    
    logger.info("Bot iniciado correctamente con todas las mejoras...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
