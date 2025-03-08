# ----- Monkey-patch para werkzeug (evita error de url_quote) -----
try:
    from werkzeug.urls import url_quote
except ImportError:
    from urllib.parse import quote as url_quote
    import werkzeug.urls
    werkzeug.urls.url_quote = url_quote
# -------------------------------------------------------------------

import os
import logging
import asyncio
import threading
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
TOKEN = os.environ.get("TELEGRAM_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Ej: "https://verduleria2.onrender.com/webhook"

if not TOKEN:
    raise ValueError("No se ha definido TELEGRAM_TOKEN en las variables de entorno.")
if not WEBHOOK_URL:
    raise ValueError("No se ha definido WEBHOOK_URL en las variables de entorno.")

# Crear la aplicación Flask
app = Flask(__name__)

# --- Crear y configurar el bot ---
# Creamos un objeto HTTPXRequest con un pool de conexiones mayor y timeout ampliado.
request_obj = HTTPXRequest(connection_pool_size=50, pool_timeout=20)
# Creamos la instancia del bot usando el objeto de request personalizado.
telegram_app = Application.builder().token(TOKEN).request(request_obj).build()

# Handler simple para /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando start iniciado")

telegram_app.add_handler(CommandHandler("start", start))
# -------------------------------------

# --- Manejo del event loop ---
# Declaramos una variable global para el loop del bot.
BOT_LOOP = None

def start_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

def ensure_bot_loop():
    global BOT_LOOP
    if BOT_LOOP is None:
        BOT_LOOP = asyncio.new_event_loop()
        t = threading.Thread(target=start_loop, args=(BOT_LOOP,), daemon=True)
        t.start()
    return BOT_LOOP
# ---------------------------------

# --- Endpoint del webhook ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    # Aseguramos que haya un event loop en segundo plano y programamos la tarea.
    loop = ensure_bot_loop()
    asyncio.run_coroutine_threadsafe(telegram_app.process_update(update), loop)
    return jsonify({"status": "ok"}), 200

# --- Endpoint opcional para configurar el webhook manualmente ---
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    success = telegram_app.bot.set_webhook(WEBHOOK_URL)
    if success:
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400
# ---------------------------------

# Inicializar la aplicación de Telegram
asyncio.run(telegram_app.initialize())
if telegram_app.bot.set_webhook(WEBHOOK_URL):
    logger.info("Webhook configurado correctamente")
else:
    logger.error("Error al configurar el webhook")

# --- Convertir la aplicación Flask (WSGI) a ASGI para usarla con Gunicorn + UvicornWorker ---
from asgiref.wsgi import WsgiToAsgi
asgi_app = WsgiToAsgi(app)
# ---------------------------------------------------------------------------------------------

# Para pruebas locales (modo WSGI con Waitress) se ejecuta lo siguiente:
if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
