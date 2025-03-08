#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import asyncio
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Configuración básica de logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise ValueError("No se encontró la variable de entorno TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
if not WEBHOOK_URL:
    raise ValueError("No se encontró la variable de entorno WEBHOOK_URL")

# Crear la aplicación de telegram
app_telegram = Application.builder().token(TOKEN).build()

# Handler del comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Comando start iniciado")
    logger.info("Se recibió /start y se respondió con 'Comando start iniciado'")

app_telegram.add_handler(CommandHandler("start", start))

# Crear la aplicación Flask (para servir el webhook)
app = Flask(__name__)

@app.route("/")
def index():
    return "OK", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    logger.info("Webhook triggered: %s", data)
    update = Update.de_json(data, app_telegram.bot)
    # Procesa la actualización de forma asíncrona
    asyncio.run(app_telegram.process_update(update))
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    # Configurar el webhook en Telegram
    result = app_telegram.bot.set_webhook(WEBHOOK_URL)
    if result:
        logger.info("Webhook configurado correctamente: %s", WEBHOOK_URL)
    else:
        logger.error("Error configurando el webhook")

    # Iniciar el servidor Flask usando waitress
    from waitress import serve
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Iniciando servidor Flask en http://0.0.0.0:{port}")
    serve(app, host="0.0.0.0", port=port)
