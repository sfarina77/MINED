import streamlit as st
import pandas as pd
from firebase_admin import credentials, firestore, initialize_app, _apps
import plotly.express as px
from datetime import datetime, timedelta

# 1. CONFIGURACIÓN DEL DASHBOARD
st.set_page_config(page_title="Monitor MINED - Gestión B2B", layout="wide", page_icon="🇸🇻")

# 2. CONEXIÓN A FIREBASE (Original que funciona en tu Mac)
if not _apps:
    try:
        cred = credentials.Certificate("firebase_llave.json")
        initialize_app(cred)
    except Exception as e:
        st.error(f"❌ Error con 'firebase_llave.json': {e}")

db = firestore.client()

# --- ESTÉTICA ---
st.title("🇸🇻 Monitor de Conectividad Escolar - MINED")
st.markdown("### Centro de Control de Infraestructura Digital | Reporte de Disponibilidad")

# --- LÓGICA DE DATOS ---
@st.cache_data(ttl=300)
def obtener_datos_completos():
    # Bajamos 5,000 registros para cubrir las 24h de múltiples sondas
    docs = db.collection("registros").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(5000).stream()
    
    lista = []
    for doc in docs:
        d = doc.to_dict()
        if 'ubicacion' in d:
            d['lat'] = d['ubicacion'].get('lat')
            d['lon'] = d['ubicacion'].get('lng')
        
        # Extraer Latencia (Ping)
        pings = d.get('pings', {})
        d['ping_mined_ms'] = pings.get('mined', -1)
        lista.append(d)
    
    df_raw = pd.DataFrame(lista)
    if df_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Inventario: Último estado de cada sonda (detecta las que murieron anoche)
    df_ultimos = df_raw.sort_values('timestamp', ascending=False).drop_duplicates('serie')
    return df_raw, df_ultimos

df_historial, df_ultimos = obtener_datos_completos()

if not df_ultimos.empty:
    # --- FILTROS Y TIEMPOS ---
    ahora = datetime.now()
    df_ultimos['fecha_dt'] = pd.to_datetime(df_ultimos['fecha'])
    umbral_20min = ahora - timedelta(minutes=20)
    
    activas = df_ultimos[df_ultimos['fecha_dt'] > umbral_20min]
    inactivas = df_ultimos[df_ultimos['fecha_dt'] <= umbral_20min]

    # --- MÉTRICAS PRINCIPALES ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Sondas con Señal", len(activas))
    m2.metric("Sondas SIN SEÑAL", len(inactivas), delta="- Requieren Atención", delta_color="inverse")
    
    alertas = len(df_ultimos[(df_ultimos['download_mbps'] < 2) & (df_ultimos['download_mbps'] > 0)])
    m3.metric("Alertas de Calidad", alertas, help="Velocidad menor a 2 Mbps", delta_color="inverse")
    
    prom_ping = activas[activas['ping_mined_ms'] > 0]['ping_mined_ms'].mean()
    m4.metric("Latencia Media", f"{prom_ping:.0f} ms" if not pd.isna(prom_ping) else "N/A")

    st.divider()

    # --- PANEL CENTRAL: MAPA Y DISPONIBILIDAD (SLA) ---
    col_izq, col_der = st.columns([2, 1])

    with col_izq:
        st.subheader("📍 Estado Geográfico (Últimas 24h)")
        fig_map = px.scatter_mapbox(df_ultimos, lat="lat", lon="lon", 
                                  color="ping_mined_ms", size="download_mbps",
                                  hover_name="serie",
                                  color_continuous_scale="RdYlGn_r",
                                  zoom=7.5, height=450)
        fig_map.update_layout(mapbox_style="carto-positron", 
                            mapbox_center={"lat": 13.6929, "lon": -89.2182})
        st.plotly_chart(fig_map, use_container_width=True)

    with col_der:
        st.subheader("📊 Disponibilidad (SLA 24h)")
        st.caption("Porcentaje de tiempo reportando datos")
        
        # Calculamos mensajes recibidos vs mensajes esperados (1440 min/día)
        df_sla = df_historial.groupby('serie').size().reset_index(name='conteo')
        df_sla['porcentaje'] = (df_sla['conteo'] / 1440 * 100).clip(upper=100)
        
        for _, row in df_sla.iterrows():
            st.write(f"ID: {row['serie']}")
            st.progress(row['porcentaje'] / 100)
            st.write(f"SLA: {row['porcentaje']:.1f}%")

    # --- SECCIÓN DE ALERTAS Y TENDENCIAS ---
    st.divider()
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.subheader("🚨 Sondas Caídas")
        if not inactivas.empty:
            for _, row in inactivas.iterrows():
                tiempo_off = ahora - row['fecha_dt']
                hrs = int(tiempo_off.total_seconds() // 3600)
                st.error(f"**{row['serie']}**\n\nDesconectada hace: {hrs} horas")
        else:
            st.success("✅ Todas las sondas están activas.")

    with c2:
        st.subheader("📈 Historial de Velocidad")
        fig_line = px.line(df_historial, x="fecha", y="download_mbps", color="serie", height=300)
        st.plotly_chart(fig_line, use_container_width=True)

    # --- TABLA Y EXPORTACIÓN ---
    st.subheader("📋 Resumen Técnico y Reporte")
    tabla_final = df_ultimos[['serie', 'fecha', 'download_mbps', 'ping_mined_ms', 'tipo_conexion']]
    tabla_final.columns = ['ID Sonda', 'Último Reporte', 'Mbps', 'Ping (ms)', 'Interface']
    st.dataframe(tabla_final, use_container_width=True)

    # Botón de Descarga
    csv = tabla_final.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Descargar Reporte CSV para MINED",
        data=csv,
        file_name=f'SLA_MINED_{datetime.now().strftime("%Y-%m-%d")}.csv',
        mime='text/csv',
    )

else:
    st.info("Esperando conexión con la base de datos...")

if st.button('🔄 Refrescar Monitor'):
    st.cache_data.clear()
    st.rerun()
