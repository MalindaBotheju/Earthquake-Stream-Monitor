"""
Streamlit dashboard -- read-only presentation layer.

Talks only to the API Gateway endpoint (never AWS directly). Two tabs:
  - Live:    recent individual quakes, a map, and any active alerts.
  - History: 6-month *summaries* -- heatmap, trend line, top quakes,
             active aftershock sequences. Never plots the raw 6-month
             table point-by-point (that's noise, not insight).

Deliberately uses Streamlit's native styling (bordered containers,
default theme, st.metric) rather than custom CSS/dark-mode overrides --
native components stay legible and consistent with zero extra styling
code to maintain.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests

st.set_page_config(
    page_title="Global Earthquake Monitor",
    page_icon="\U0001F30D",
    layout="wide",
)

API_BASE_URL = st.secrets.get("API_BASE_URL", os.environ.get("API_BASE_URL", ""))
ACCENT_SCALE = "OrRd"


@st.cache_data(ttl=300)  # 5 min cache -- ingest only runs hourly anyway
def get(path: str, **params):
    if not API_BASE_URL:
        st.error("API_BASE_URL is not set. Add it under Settings -> Secrets.")
        st.stop()
    resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


st.title("\U0001F30D Global Earthquake Stream Monitor")
st.caption("USGS feed \u2192 serverless AWS pipeline \u2192 this dashboard. Updates roughly hourly.")

tab_live, tab_history = st.tabs(["\U0001F534 Live", "\U0001F4C8 History (6 months)"])

# ---------------------------------------------------------------- Live ---
with tab_live:
    recent = get("/recent", days=7).get("quakes", [])
    alerts = get("/alerts").get("alerts", [])

    df = pd.DataFrame(recent) if recent else pd.DataFrame(columns=["mag", "lat", "lon", "place", "time", "depth_km", "tsunami"])
    if not df.empty:
        df["mag"] = df["mag"].astype(float)
        df["marker_size"] = df["mag"].clip(lower=0.1)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Quakes (7d)", f"{len(df):,}")
    m2.metric("Strongest", f"M {df['mag'].max():.1f}" if not df.empty else "\u2013")
    m3.metric("Tsunami flags", int(df["tsunami"].sum()) if not df.empty and "tsunami" in df else 0)
    m4.metric("Active alerts", len(alerts))

    st.write("")
    col1, col2 = st.columns([2, 1])

    with col1:
        with st.container(border=True):
            st.subheader("Recent activity map")
            if not df.empty:
                fig = px.scatter_geo(
                    df, lat="lat", lon="lon", size="marker_size", color="mag",
                    hover_name="place", color_continuous_scale=ACCENT_SCALE,
                    projection="natural earth", labels={"mag": "Magnitude"},
                )
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=460)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No recent quakes in the last 7 days above the feed threshold.")

    with col2:
        with st.container(border=True):
            st.subheader("\u26a0\ufe0f Active alerts")
            if alerts:
                for a in alerts[:10]:
                    st.warning(f"**{a['region']}**  \n{a['details']}")
            else:
                st.success("No unusual regional activity right now.")

        if not df.empty:
            with st.container(border=True):
                st.subheader("Magnitude distribution")
                hist = px.histogram(df, x="mag", nbins=20, color_discrete_sequence=["#d62728"])
                hist.update_layout(
                    margin=dict(l=0, r=0, t=10, b=0), height=200,
                    showlegend=False, xaxis_title="Magnitude", yaxis_title="",
                )
                st.plotly_chart(hist, use_container_width=True)

    st.write("")
    with st.container(border=True):
        st.subheader("Feed")
        if not df.empty:
            df_show = df[["time", "place", "mag", "depth_km", "tsunami"]].sort_values("time", ascending=False).copy()
            df_show["time"] = pd.to_datetime(df_show["time"], unit="ms")
            st.dataframe(
                df_show, use_container_width=True, hide_index=True,
                column_config={
                    "time": st.column_config.DatetimeColumn("Time", format="MMM D, HH:mm"),
                    "place": st.column_config.TextColumn("Location"),
                    "mag": st.column_config.ProgressColumn("Magnitude", min_value=0, max_value=8, format="%.1f"),
                    "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.1f"),
                    "tsunami": st.column_config.CheckboxColumn("Tsunami flag"),
                },
            )

# ------------------------------------------------------------- History ---
with tab_history:
    heatmap = get("/history/heatmap").get("regions", [])
    trend = get("/history/trend").get("trend", [])
    top_quakes = get("/history/top-quakes", limit=10).get("top_quakes", [])
    clusters = get("/clusters").get("sequences", [])

    hcol1, hcol2 = st.columns(2)

    with hcol1:
        with st.container(border=True):
            st.subheader("Activity heatmap by region")
            if heatmap:
                hdf = pd.DataFrame(heatmap)
                hdf[["lat", "lon"]] = hdf["region"].str.split("_", expand=True).astype(float)
                fig = px.density_map(
                    hdf, lat="lat", lon="lon", z="total_count", radius=35,
                    map_style="carto-positron", zoom=0, color_continuous_scale=ACCENT_SCALE,
                )
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=360)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough history yet to render a heatmap.")

    with hcol2:
        with st.container(border=True):
            st.subheader("Quake count trend")
            if trend:
                tdf = pd.DataFrame(trend)
                tdf["date"] = pd.to_datetime(tdf["date"])
                fig = px.area(tdf, x="date", y="count", color_discrete_sequence=["#d62728"])
                fig.update_layout(
                    margin=dict(l=0, r=0, t=10, b=0), height=360,
                    xaxis_title="", yaxis_title="Quakes / day",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough history yet to render a trend line.")

    hcol3, hcol4 = st.columns(2)

    with hcol3:
        with st.container(border=True):
            st.subheader("Biggest quakes (6 months)")
            if top_quakes:
                tqdf = pd.DataFrame(top_quakes)[["time", "place", "mag", "depth_km"]].copy()
                tqdf["time"] = pd.to_datetime(tqdf["time"], unit="ms")
                st.dataframe(
                    tqdf, use_container_width=True, hide_index=True,
                    column_config={
                        "time": st.column_config.DatetimeColumn("Time", format="MMM D, YYYY"),
                        "place": st.column_config.TextColumn("Location"),
                        "mag": st.column_config.ProgressColumn("Magnitude", min_value=0, max_value=8, format="%.1f"),
                        "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.1f"),
                    },
                )
            else:
                st.info("No quakes recorded yet.")

    with hcol4:
        with st.container(border=True):
            st.subheader("Active aftershock sequences")
            if clusters:
                cdf = pd.DataFrame(clusters)[["region", "mainshock_mag", "quake_count", "max_mag"]].copy()
                cdf.columns = ["Region", "Mainshock mag", "Quake count", "Max mag"]
                st.dataframe(cdf, use_container_width=True, hide_index=True)
            else:
                st.info("No multi-quake sequences currently tracked.")