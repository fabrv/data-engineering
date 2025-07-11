import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Database path
DB_PATH = Path("/app/data/database.db")

# Page configuration
st.set_page_config(
    page_title="CitiBike Analytics Dashboard",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_db_connection():
    """Create database connection"""
    if not DB_PATH.exists():
        st.error(f"Database not found at {DB_PATH}")
        st.stop()
    return sqlite3.connect(str(DB_PATH))

def load_trips_per_station_day():
    """Load top 10 stations by total trips"""
    conn = get_db_connection()
    query = """
    SELECT 
        start_station_name,
        SUM(trip_count) as total_trips
    FROM trips_per_station_day 
    WHERE start_station_name IS NOT NULL
    GROUP BY start_station_name
    ORDER BY total_trips DESC
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def load_avg_duration_by_hour():
    """Load average trip duration by hour"""
    conn = get_db_connection()
    query = """
    SELECT 
        CAST(hour AS INTEGER) as hour,
        avg_duration,
        trip_count
    FROM avg_duration_by_hour
    ORDER BY hour
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def load_top_stations_monthly():
    """Load top 10 stations from most recent month"""
    conn = get_db_connection()
    query = """
    SELECT 
        start_station_name,
        trip_count,
        year_month
    FROM top_stations_monthly
    WHERE year_month = (SELECT MAX(year_month) FROM top_stations_monthly)
    AND rank <= 10
    ORDER BY rank
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def load_user_type_summary():
    """Load user type summary data"""
    conn = get_db_connection()
    query = """
    SELECT 
        user_type,
        total_trips,
        avg_duration,
        min_duration,
        max_duration
    FROM user_type_summary
    WHERE user_type IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def create_station_trips_chart(df):
    """Create bar chart for top stations by total trips"""
    fig = px.bar(
        df,
        x='total_trips',
        y='start_station_name',
        orientation='h',
        title='Top 10 Stations by Total Trips',
        labels={'total_trips': 'Total Trips', 'start_station_name': 'Station Name'},
        color='total_trips',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=500,
        yaxis={'categoryorder': 'total ascending'},
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14
    )
    
    return fig

def create_hourly_duration_chart(df):
    """Create bar chart for average duration by hour"""
    fig = px.bar(
        df,
        x='hour',
        y='avg_duration',
        title='Average Trip Duration by Hour of Day',
        labels={'hour': 'Hour of Day', 'avg_duration': 'Average Duration (seconds)'},
        color='avg_duration',
        color_continuous_scale='Greens'
    )
    
    fig.update_layout(
        height=400,
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        xaxis=dict(tickmode='linear', tick0=0, dtick=1)
    )
    
    # Add hover information
    fig.update_traces(
        hovertemplate='<b>Hour:</b> %{x}<br><b>Avg Duration:</b> %{y:.0f} seconds<br><extra></extra>'
    )
    
    return fig

def create_monthly_stations_chart(df):
    """Create bar chart for top monthly stations"""
    if not df.empty:
        month = df['year_month'].iloc[0]
        fig = px.bar(
            df,
            x='trip_count',
            y='start_station_name',
            orientation='h',
            title=f'Top 10 Stations for {month}',
            labels={'trip_count': 'Trip Count', 'start_station_name': 'Station Name'},
            color='trip_count',
            color_continuous_scale='Oranges'
        )
        
        fig.update_layout(
            height=500,
            yaxis={'categoryorder': 'total ascending'},
            title_font_size=16,
            xaxis_title_font_size=14,
            yaxis_title_font_size=14
        )
    else:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False)
    
    return fig

def create_user_type_chart(df):
    """Create bar chart for user type summary"""
    fig = px.bar(
        df,
        x='user_type',
        y='total_trips',
        title='Total Trips by User Type',
        labels={'user_type': 'User Type', 'total_trips': 'Total Trips'},
        color='total_trips',
        color_continuous_scale='Purples'
    )
    
    fig.update_layout(
        height=400,
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14
    )
    
    return fig

def main():
    # Header
    st.title("🚴 CitiBike Analytics Dashboard")
    st.markdown("Real-time insights from CitiBike trip data")
    
    # Sidebar
    st.sidebar.header("📊 Dashboard Controls")
    refresh_data = st.sidebar.button("🔄 Refresh Data")
    
    # Database info
    if DB_PATH.exists():
        st.sidebar.success(f"✅ Connected to database")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trips")
        total_trips = cursor.fetchone()[0]
        st.sidebar.metric("Total Trips in DB", f"{total_trips:,}")
        conn.close()
    else:
        st.sidebar.error("❌ Database not found")
        return
    
    # Main content
    try:
        # Load data
        with st.spinner("Loading data..."):
            station_trips_df = load_trips_per_station_day()
            hourly_duration_df = load_avg_duration_by_hour()
            monthly_stations_df = load_top_stations_monthly()
            user_type_df = load_user_type_summary()
        
        # Layout in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(
                create_station_trips_chart(station_trips_df),
                use_container_width=True
            )
            
            st.plotly_chart(
                create_hourly_duration_chart(hourly_duration_df),
                use_container_width=True
            )
        
        with col2:
            st.plotly_chart(
                create_monthly_stations_chart(monthly_stations_df),
                use_container_width=True
            )
            
            st.plotly_chart(
                create_user_type_chart(user_type_df),
                use_container_width=True
            )
        
        # Data summary section
        st.header("Data")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Top Station Trips",
                f"{station_trips_df['total_trips'].max():,}" if not station_trips_df.empty else "N/A"
            )
        
        with col2:
            st.metric(
                "Peak Hour Duration",
                f"{hourly_duration_df['avg_duration'].max():.0f}s" if not hourly_duration_df.empty else "N/A"
            )
        
        with col3:
            st.metric(
                "User Types",
                len(user_type_df) if not user_type_df.empty else "N/A"
            )
        
        with col4:
            most_recent_month = monthly_stations_df['year_month'].iloc[0] if not monthly_stations_df.empty else "N/A"
            st.metric(
                "Latest Month",
                most_recent_month
            )
        
        # Raw data expanders
        with st.expander("Top Stations"):
            st.dataframe(station_trips_df, use_container_width=True)
        
        with st.expander("Hourly Patterns"):
            st.dataframe(hourly_duration_df, use_container_width=True)
        
        with st.expander("Monthly Top Stations"):
            st.dataframe(monthly_stations_df, use_container_width=True)
        
        with st.expander("User Types"):
            st.dataframe(user_type_df, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error loading dashboard: {str(e)}")
        st.info("Make sure the ETL pipeline has run successfully and created the gold tables.")

if __name__ == "__main__":
    main()