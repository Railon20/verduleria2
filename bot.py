import os
import logging
import asyncio
import threading
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from waitress import serve

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
TOKEN = os.environ.get("TELEGRAM_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Ejemplo: "https://<tu-subdominio>.onrender.com/webhook"

if not TOKEN:
    raise ValueError("No se ha definido TELEGRAM_TOKEN en las variables de entorno.")
if not WEBHOOK_URL:
    raise ValueError("No se ha definido WEBHOOK_URL en las variables de entorno.")

# Crear la aplicación Flask y la instancia de Telegram
app = Flask(__name__)
telegram_app = Application.builder().token(TOKEN).build()

# --- Iniciar un event loop global en un hilo separado ---
# Creamos un bucle de eventos nuevo
event_loop = asyncio.new_event_loop()

def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

# Iniciamos el bucle de eventos en un hilo independiente (esto se hace al iniciar Flask)
@app.before_first_request
def init_event_loop():
    threading.Thread(target=start_event_loop, args=(event_loop,), daemon=True).start()
    # Configuramos el webhook
    if telegram_app.bot.set_webhook(WEBHOOK_URL):
        logger.info("Webhook configurado correctamente")
    else:
        logger.error("Error al configurar el webhook")

# --- Handler para el comando /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Comando start iniciado")

telegram_app.add_handler(CommandHandler("start", start))

# --- Endpoint del webhook ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    # Encolamos la tarea para procesar la actualización en el event loop global
    asyncio.run_coroutine_threadsafe(telegram_app.process_update(update), event_loop)
    return jsonify({"status": "ok"}), 200

# --- (Opcional) Endpoint para configurar manualmente el webhook ---
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    if telegram_app.bot.set_webhook(WEBHOOK_URL):
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400

# --- Ejecutar la aplicación Flask con Waitress ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
