FROM python:3.10-slim
WORKDIR /app
RUN pip install --no-cache-dir pyrogram==2.0.106 motor==3.3.2 pymongo==4.6.3 aiohttp
COPY bot.py .
CMD ["python", "bot.py"]
