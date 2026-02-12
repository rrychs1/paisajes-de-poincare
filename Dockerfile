FROM python:3.11-slim

# Establecer zona horaria para logs
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no-root
RUN useradd -m -u 1000 botuser

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements y instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del bot
COPY --chown=botuser:botuser . .

# Crear directorios necesarios
RUN mkdir -p /app/data /app/logs && chown -R botuser:botuser /app

# Cambiar a usuario no-root
USER botuser

# Comando de inicio
CMD ["python", "main.py"]
