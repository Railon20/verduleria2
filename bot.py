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
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Ejemplo: "https://verduleria2-co0u.onrender.com/webhook"
if not TOKEN:
    raise ValueError("No se ha definido TELEGRAM_TOKEN en las variables de entorno.")
if not WEBHOOK_URL:
    raise ValueError("No se ha definido WEBHOOK_URL en las variables de entorno.")

# Importar Request (para configurar el pool de conexiones)
#try:
#    from telegram.request import Request
#except ImportError:
#    from telegram._request import Request

# Creamos el objeto Request con un pool mayor y tiempos de espera configurados
#req = Request(con_pool_size=20, connect_timeout=10, read_timeout=10)

# Creamos la aplicación Flask y la instancia de Telegram usando el Request personalizado
app = Flask(__name__)
#telegram_app = Application.builder().token(TOKEN).request(req).build()
telegram_app = Application.builder().token(TOKEN).build()


# --- Iniciar un event loop global en un hilo separado ---
event_loop = asyncio.new_event_loop()

def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

# Al iniciar la aplicación, arrancamos el event loop y configuramos el webhook
@app.before_first_request
def init_event_loop_and_webhook():
    threading.Thread(target=start_event_loop, args=(event_loop,), daemon=True).start()
    if telegram_app.bot.set_webhook(WEBHOOK_URL):
        logger.info("Webhook configurado correctamente")
    else:
        logger.error("Error al configurar el webhook")

# --- Handler para el comando /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Dentro del handler /start")
    await update.message.reply_text("Comando start iniciado")


telegram_app.add_handler(CommandHandler("start", start))

# --- Endpoint para recibir las actualizaciones del webhook ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    logger.info("Webhook recibido: %s", data)
    update = Update.de_json(data, telegram_app.bot)
    future = asyncio.run_coroutine_threadsafe(telegram_app.process_update(update), event_loop)
    try:
        # Espera hasta 10 segundos para que se procese el update
        future.result(timeout=10)
    except Exception as e:
        logger.error("Error al procesar el update: %s", e)
    return jsonify({"status": "ok"}), 200


# --- (Opcional) Endpoint para configurar manualmente el webhook ---
@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    if telegram_app.bot.set_webhook(WEBHOOK_URL):
        return "Webhook configurado correctamente", 200
    else:
        return "Error configurando webhook", 400

# --- Ejecutar la aplicación Flask usando Waitress ---
if __name__ == "__main__":
    # Inicia el event loop en un hilo de fondo
    t = threading.Thread(target=start_event_loop, args=(event_loop,), daemon=True)
    t.start()
    # Configura el webhook usando run_coroutine_threadsafe para awaitear el coroutine
    future = asyncio.run_coroutine_threadsafe(telegram_app.bot.set_webhook(WEBHOOK_URL), event_loop)
    try:
        if future.result(timeout=10):
            logger.info("Webhook configurado correctamente")
        else:
            logger.error("Error al configurar el webhook")
    except Exception as e:
        logger.error("Excepción al configurar el webhook: %s", e)
    # Arranca el servidor de Waitress
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)

