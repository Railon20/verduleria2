# ----- Monkey-patch para werkzeug ----- 
# Esto debe ir antes de cualquier importación de Flask.
try:
    from werkzeug.urls import url_quote
except ImportError:
    from urllib.parse import quote as url_quote
    import werkzeug.urls
    werkzeug.urls.url_quote = url_quote

# ---------------------------------------

import os
import logging
import asyncio
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
TOKEN = os.environ.get("TELEGRAM_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Ejemplo: "https://verduleria2.onrender.com/webhook"

if not TOKEN:
    raise ValueError("No se ha definido TELEGRAM_TOKEN en las variables de entorno.")
if not WEBHOOK_URL:
    raise ValueError("No se ha definido WEBHOOK_URL en las variables de entorno.")

# Crear la aplicación Flask y la instancia del bot de Telegram
app = Flask(__name__)
telegram_app = Application.builder().token(TOKEN).build()

# Handler simple para /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando start iniciado")

telegram_app.add_handler(CommandHandler("start", start))

# Endpoint para recibir actualizaciones vía webhook
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    # Procesar el update (como estamos en un contexto síncrono usamos asyncio.run)
    asyncio.run(telegram_app.process_update(update))
    return jsonify({"status": "ok"}), 200

# (Opcional) Endpoint para configurar el webhook manualmente
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    success = telegram_app.bot.set_webhook(WEBHOOK_URL)
    if success:
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400

# Inicializar la aplicación de Telegram (esto es necesario para que process_update funcione)
asyncio.run(telegram_app.initialize())
if telegram_app.bot.set_webhook(WEBHOOK_URL):
    logger.info("Webhook configurado correctamente")
else:
    logger.error("Error al configurar el webhook")

# Convertir la aplicación Flask (WSGI) a ASGI (para que Gunicorn con UvicornWorker la pueda servir)
from asgiref.wsgi import WsgiToAsgi
asgi_app = WsgiToAsgi(app)

# Para pruebas locales (modo WSGI) puedes usar waitress
if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
