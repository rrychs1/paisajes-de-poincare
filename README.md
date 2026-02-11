# paisajes-de-poincare

Bot de trading algorítmico en Python para derivados de criptomonedas con detección de régimen de mercado, gestión de riesgo avanzada y enrutamiento entre estrategias de rango (grid) y tendencia.[page:3][page:2]

> ⚠️ Aviso: Este proyecto es experimental y no constituye asesoría financiera. Úsalo bajo tu propia responsabilidad.

---

## Características

- Detección de régimen de mercado por símbolo (tendencia, rango, desconocido) usando indicadores técnicos (ADX, EMAs, Bandas de Bollinger).[page:3]  
- Enrutamiento automático de señales según el régimen mediante un `StrategyRouter` (estrategias de grid y de tendencia).[page:3]  
- Gestión de riesgo centralizada (`RiskManager`): tamaño de posición por porcentaje de riesgo, límite de apalancamiento, límite de tamaño máximo de posición y kill switch por pérdida diaria.[page:3]  
- Sincronización de trades con el exchange, cálculo de PnL realizado y registro persistente en base de datos SQLite (`bot_state.db`).[page:3]  
- Mecanismo de transición entre estrategias GRID→TREND y TREND→GRID con pasos explícitos (cancelación de órdenes, stops de emergencia, bloqueo/desbloqueo de grid).[page:3]  
- Sistema de alertas configurable vía webhook (mensajes sobre cambios de régimen, PnL, kill switch, errores, etc.).[page:3][page:2]  
- Métricas internas sobre ciclos, señales, órdenes y errores para observabilidad.[page:3]

---

## Estructura del proyecto

```text
.
├── common/          # Alertas, métricas, tipos comunes
├── config/          # Configuración (settings, logging)
├── data/            # Motor de datos y base de datos (SQLite)
├── execution/       # Wrapper de exchange, órdenes, transiciones
├── indicators/      # Cálculo de indicadores técnicos
├── regime/          # Detector de régimen de mercado
├── risk/            # Gestión de riesgo y sizing
├── strategies/      # Estrategias de trading y router
├── systemd/         # Archivos de servicio para despliegue
├── main.py          # Punto de entrada del bot
├── requirements.txt # Dependencias de Python
└── .env.example     # Variables de entorno de ejemplo

