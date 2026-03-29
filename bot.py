import os
import ssl
import certifi
import logging
import sqlite3
from html import escape
import asyncio
from datetime import datetime, timedelta, timezone
from telegram import Update, ChatPermissions, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, filters, JobQueue
import requests
import aiohttp
from functools import lru_cache

# =========================================================
# CONFIGURACIÓN
# =========================================================

BOT_TOKEN = "8752924542:AAH9_zy4Sa2cE7AAqJi9bZHshYDF1Jh6ggo"
MAIN_GROUP_ID = -1003777076233
REFE_GROUP_ID = -1003566231864
LOG_GROUP_ID = -5154377611
SCAM_GROUP_ID = -1003810336549  # Reemplaza con el ID de tu grupo
SUPERADMIN_IDS = {8747380388}
AUTHORIZED_ADMIN_IDS = {5441572575,8747380388}
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
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", level=logging.INFO)
logger = logging.getLogger("vip_bot")

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
    c.execute("""
    CREATE TABLE IF NOT EXISTS plans(
        telegram_id INTEGER PRIMARY KEY,
        end_date TEXT,
        last_day_warn_sent INTEGER DEFAULT 0
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS refe_stats(
        sender_id INTEGER PRIMARY KEY,
        count INTEGER DEFAULT 0
    )
    """)
    conn.commit()
    conn.close()

    ensure_column_exists("users", "cookie", "TEXT")
    ensure_column_exists("plans", "last_day_warn_sent", "INTEGER DEFAULT 0")
    set_setting_if_missing("refe_text", DEFAULT_REFE_TEXT)

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

async def safe_reply(message, text: str):
    try:
        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error al enviar mensaje: {e}")
        await message.reply_text(text)

async def send_log(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not LOG_GROUP_ID:
        return
    try:
        await context.bot.send_message(LOG_GROUP_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning("No se pudo mandar log: %s", e)

# =========================================================
# VERIFICACIÓN DE TARJETAS (MULTIPLE)
# =========================================================

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

async def mx_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    user_row = get_user_row(user_id)

    if not user_row or not user_row["cookie"]:
        await safe_reply(msg, "❌ <b>Error:</b> Necesitas una cookie guardada. Usa <code>/cuki</code> para guardarla.")
        return

    cookie = user_row["cookie"]

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
        await safe_reply(msg, "❌ <b>Uso:</b> Responde al mensaje con las tarjetas o escribe <code>/mx</code> seguido de las tarjetas.")
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
        await safe_reply(msg, "❌ <b>Error:</b> No se encontraron tarjetas válidas en el formato: <code>numero|mes|año|cvv</code>\n\nEjemplo:\n5317223259757842|11|2033|030")
        return

    await safe_reply(msg, f"🔍 <b>Verificando {len(cards_to_check)} tarjetas...</b>\n\nEspera un momento...")

    results = []
    for card in cards_to_check:
        result = await verify_card(card, cookie)
        results.append(result)

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
        output_text += f"<i>{message}</i>\n\n"

    output_text += f"━━━━━━━━━━━━━━━━\n"
    output_text += f"✅ <b>Aprobadas:</b> {approved_count}\n"
    output_text += f"❌ <b>Declinadas:</b> {declined_count}\n"
    output_text += f"⚠️ <b>Errores:</b> {error_count}\n"
    output_text += f"━━━━━━━━━━━━━━━━"

    await safe_reply(msg, output_text)

# =========================================================
# MANEJADORES DE COMANDOS (RESTO DEL CÓDIGO)
# =========================================================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user_registered(user)
    await safe_reply(update.message, f"👋 Bienvenido, {user.first_name}!\n\nUsa <code>/help</code> para ver los comandos.")

async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = (
        "🤖 <b>COMANDOS DISPONIBLES</b>\n\n"
        "<b>Usuarios:</b>\n"
        "• <code>/start</code> - Iniciar bot\n"
        "• <code>/help</code> - Ver comandos\n"
        "• <code>/mi_plan</code> - Ver estado de tu plan\n"
        "• <code>/ck</code> - Ver cookie guardada\n"
        "• <code>/cuki</code> - Guardar cookie (responde al mensaje)\n"
        "• <code>/mx</code> - Verificar tarjetas (envía varias)\n"
        "• <code>/refe</code> - Enviar referencia\n"
        "• <code>/staff</code> - Ver staff\n"
        "• <code>/precios</code> - Ver precios\n"
        "• <code>/id</code> - Ver tu ID\n"
    )
    if is_admin(user.id):
        text += "\n\n<b>Admins:</b>\n• <code>/panel</code> - Panel de control\n• <code>/plan ID dias</code> - Dar plan\n• <code>/ban ID</code> - Banear\n• <code>/warn ID</code> - Advertir"
    await safe_reply(update.message, text)

async def mi_plan_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    status = "Activo" if plan_active(user_id) else "Inactivo"
    remaining = plan_remaining(user_id)
    await safe_reply(update.message, f"📅 <b>ESTADO DEL PLAN</b>\n\nEstado: <code>{status}</code>\nRestante: <code>{remaining}</code>")

async def ck_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    row = get_user_row(user_id)
    cookie = row["cookie"] if row else ""
    if cookie:
        await safe_reply(update.message, f"🍪 <b>TU COOKIE:</b>\n\n<code>{cookie}</code>")
    else:
        await safe_reply(update.message, "❌ No tienes cookies guardadas. Usa <code>/cuki</code> para guardarlas.")

async def cuki_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = None

    if msg.reply_to_message:
        text = msg.reply_to_message.text
    elif msg.text:
        text = msg.text.split(' ', 1)[1] if len(msg.text.split(' ')) > 1 else None

    if text:
        user_id = update.effective_user.id
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET cookie=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        await safe_reply(msg, "✅ <b>Cookie guardada correctamente.</b>")
    else:
        await safe_reply(msg, "❌ <b>Error:</b> Responde al mensaje con la cookie o escribe <code>/cuki TU_COOKIE</code>")

async def refe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    user = update.effective_user

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

        await safe_reply(msg, "✅ <b>Referencia enviada correctamente.</b>\n\n"
                          f"📊 <b>Tu número de referencia:</b> <code>{ref_count}</code>")
    except Exception as e:
        logger.error(f"Error al enviar referencia: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo enviar la referencia. Intenta nuevamente.")

async def staff_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👑 <b>STAFF OFICIAL</b>\n\n"
        "• <b>ıllıllıᐯ卂乂ıllıllı</b>:@TheVax1\n"
        "• <b>Elcaza</b>: @ElcazaJR1\n"
    )
    await safe_reply(update.message, text)

async def precios_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "💎 <b>PRECIOS</b>\n\n"
        "• 7 días: <code>$200 MXN</code>\n"
        "• 15 días: <code>$350 MXN</code>\n"
        "• 30 días: <code>$500 MXN</code>"
    )
    await safe_reply(update.message, text)

async def id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = f"🆔 <b>TU ID:</b> <code>{user_id}</code>\n\n🏢 <b>ID DEL GRUPO:</b> <code>{chat_id}</code>"
    await safe_reply(update.message, text)

async def idgr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await safe_reply(update.message, f"🏢 <b>ID DEL GRUPO:</b> <code>{chat_id}</code>")

# =========================================================
# PANEL ADMIN (BÁSICO)
# =========================================================

async def panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await safe_reply(update.message, "❌ No autorizado.")
        return
    await safe_reply(update.message, "🎛 <b>PANEL ADMIN</b>\n\nFuncionalidad disponible en la versión completa.")

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
    try:
        target_id = int(args[0])
        days = int(args[1])
        end_date = now_mx() + timedelta(days=days)
        conn = db()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO plans (telegram_id, end_date) VALUES (?, ?)", (target_id, end_date.isoformat()))
        conn.commit()
        conn.close()
        await safe_reply(msg, f"✅ <b>Plan asignado:</b> <code>{days} días</code> a <code>{target_id}</code>")
        await send_log(context, f"Admin {user.id} asignó {days} días de plan a {target_id}")
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
    try:
        target_id = int(args[0])
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE users SET banned=1 WHERE telegram_id=?", (target_id,))
        conn.commit()
        conn.close()
        await safe_reply(msg, f"✅ <b>Usuario {target_id} baneado.</b>")
        await send_log(context, f"Admin {user.id} baneó a {target_id}")
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
    except Exception as e:
        logger.error(f"Error al advertir usuario: {e}")
        await safe_reply(msg, "❌ <b>Error:</b> No se pudo advertir al usuario. Revisa los logs para más detalles.")

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
        if end_date.date() == now.date() and not last_day_warn_sent:
            try:
                await context.bot.send_message(user_id, "⚠️ <b>Tu plan expira mañana.</b>")
                c.execute("UPDATE plans SET last_day_warn_sent=1 WHERE telegram_id=?", (user_id,))
                conn.commit()
            except Exception as e:
                logger.warning(f"Error al enviar aviso de expiración a {user_id}: {e}")
        elif end_date < now:
            try:
                await context.bot.kick_chat_member(chat_id=MAIN_GROUP_ID, user_id=user_id)
                c.execute("DELETE FROM plans WHERE telegram_id=?", (user_id,))
                conn.commit()
            except Exception as e:
                logger.warning(f"Error al expulsar a {user_id}: {e}")
    conn.close()

# =========================================================
# INICIALIZACIÓN
# =========================================================

def main():
    init_db()

    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers de Usuarios
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CommandHandler("mi_plan", mi_plan_handler))
    application.add_handler(CommandHandler("ck", ck_handler))
    application.add_handler(CommandHandler("cuki", cuki_handler))
    application.add_handler(CommandHandler("mx", mx_handler))
    application.add_handler(CommandHandler("refe", refe_handler))
    application.add_handler(CommandHandler("staff", staff_handler))
    application.add_handler(CommandHandler("precios", precios_handler))
    application.add_handler(CommandHandler("id", id_handler))
    application.add_handler(CommandHandler("idgr", idgr_handler))

    # Handlers de Admin
    application.add_handler(CommandHandler("panel", panel_cmd))
    application.add_handler(CommandHandler("plan", plan_cmd))
    application.add_handler(CommandHandler("ban", ban_cmd))
    application.add_handler(CommandHandler("warn", warn_cmd))

    # JobQueue para tareas periódicas
    job_queue = application.job_queue
    job_queue.run_repeating(check_plans, interval=timedelta(minutes=1), first=0)



    logger.info("Bot iniciado correctamente...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
