import os
import logging
import asyncio
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

# Creamos la aplicación Flask
app = Flask(__name__)

# Configuramos un objeto HTTPXRequest con pool aumentado (para evitar problemas de timeout)
request_obj = HTTPXRequest(connection_pool_size=100, pool_timeout=60)

# Creamos la instancia de la aplicación de Telegram usando nuestro request personalizado
telegram_app = Application.builder().token(TOKEN).request(request_obj).build()

# Handler simple para /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando start iniciado")

telegram_app.add_handler(CommandHandler("start", start))

# Endpoint para recibir actualizaciones vía webhook (ya no usamos await con get_json)
@app.route("/webhook", methods=["POST"])
async def webhook():
    data = request.get_json()  # Se elimina el await
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return jsonify({"status": "ok"}), 200

# (Opcional) Endpoint para configurar el webhook manualmente
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    success = telegram_app.bot.set_webhook(WEBHOOK_URL)
    if success:
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400

# Inicializamos la aplicación de Telegram (esto debe hacerse antes de convertir Flask a ASGI)
asyncio.run(telegram_app.initialize())

# Convertimos la aplicación Flask a ASGI para que Gunicorn pueda usarla con uvicorn.workers.UvicornWorker
from asgiref.wsgi import WsgiToAsgi
asgi_app = WsgiToAsgi(app)

# Si se ejecuta localmente, se puede usar Waitress:
if __name__ == "__main__":
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
