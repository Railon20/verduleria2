import os
import logging
import asyncio
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
# Importamos Request desde la ruta interna
from telegram.request.request import Request

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
TOKEN = os.environ.get("TELEGRAM_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Ejemplo: "https://verduleria2.onrender.com/webhook"

if not TOKEN:
    raise ValueError("No se ha definido TELEGRAM_TOKEN en las variables de entorno.")
if not WEBHOOK_URL:
    raise ValueError("No se ha definido WEBHOOK_URL en las variables de entorno.")

# Crear la aplicación Flask
app = Flask(__name__)

# Crear un objeto Request personalizado con mayor pool y timeout
req = Request(con_pool_size=100, pool_timeout=30)

# Crear la aplicación de Telegram usando el Request personalizado
telegram_app = Application.builder().token(TOKEN).request(req).build()

# Handler simple para /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando start iniciado")

telegram_app.add_handler(CommandHandler("start", start))

# Endpoint para recibir actualizaciones vía webhook (definido como async para usarse con ASGI)
@app.route("/webhook", methods=["POST"])
async def webhook():
    data = request.get_json()
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return jsonify({"status": "ok"}), 200

# (Opcional) Endpoint para configurar manualmente el webhook
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    success = telegram_app.bot.set_webhook(WEBHOOK_URL)
    if success:
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400

# Inicializar la aplicación de Telegram (esto se hace una vez al inicio)
asyncio.run(telegram_app.initialize())

# Convertir la aplicación Flask a ASGI para que Gunicorn (con worker uvicorn) la ejecute
from asgiref.wsgi import WsgiToAsgi
asgi_app = WsgiToAsgi(app)

# Si se ejecuta directamente (modo Waitress para pruebas locales)
if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
