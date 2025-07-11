import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os
import time
import shutil

# Database paths
ORIGINAL_DB_PATH = Path(os.getenv('DB_PATH', '/app/data/database.db'))
READONLY_DB_PATH = Path('/tmp/readonly_database.db')

# Page configuration
st.set_page_config(
    page_title="CitiBike Analytics Dashboard",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded"
)

def create_readonly_copy():
    """Create a read-only copy of the database in /tmp"""
    try:
        if not ORIGINAL_DB_PATH.exists():
            return False, "Original database not found"
        
        # Check if we need to update the copy
        need_update = True
        if READONLY_DB_PATH.exists():
            original_mtime = ORIGINAL_DB_PATH.stat().st_mtime
            copy_mtime = READONLY_DB_PATH.stat().st_mtime
            need_update = original_mtime > copy_mtime
        
        if need_update:
            st.info("📋 Creating read-only database copy...")
            shutil.copy2(str(ORIGINAL_DB_PATH), str(READONLY_DB_PATH))
            # Make it read-only
            os.chmod(READONLY_DB_PATH, 0o444)
        
        return True, "Read-only copy ready"
        
    except Exception as e:
        return False, f"Failed to create copy: {str(e)}"

def get_db_connection(timeout=30):
    """Create database connection using read-only copy"""
    try:
        if not READONLY_DB_PATH.exists():
            raise FileNotFoundError("Read-only database copy not found")
        
        # Connect to the read-only copy
        conn = sqlite3.connect(
            str(READONLY_DB_PATH), 
            timeout=timeout,
            check_same_thread=False
        )
        
        # Set pragmas for performance
        conn.execute("PRAGMA cache_size=10000;")
        conn.execute("PRAGMA temp_store=memory;")
        
        return conn
        
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        raise

def test_database_connection():
    """Test if we can connect and query the database"""
    try:
        conn = get_db_connection(timeout=5)
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return True, f"Connected successfully. Found {table_count} tables"
        
    except Exception as e:
        return False, f"Connection test failed: {str(e)}"

def safe_query_execution(query, params=None, query_name="Unknown"):
    """Execute query with proper error handling"""
    conn = None
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn, params=params)
        return df
        
    except Exception as e:
        st.error(f"Query {query_name} failed: {str(e)}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def load_trips_per_station_day():
    """Load top 10 stations by total trips"""
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
    return safe_query_execution(query, query_name="trips_per_station_day")

def load_avg_duration_by_hour():
    """Load average trip duration by hour"""
    query = """
    SELECT 
        CAST(hour AS INTEGER) as hour,
        avg_duration,
        trip_count
    FROM avg_duration_by_hour
    ORDER BY hour
    LIMIT 24
    """
    return safe_query_execution(query, query_name="avg_duration_by_hour")

def load_top_stations_monthly():
    """Load top 10 stations from most recent month"""
    query = """
    SELECT 
        start_station_name,
        trip_count,
        year_month
    FROM top_stations_monthly
    WHERE rank <= 10
    ORDER BY year_month DESC, rank ASC
    LIMIT 10
    """
    return safe_query_execution(query, query_name="top_stations_monthly")

def load_user_type_summary():
    """Load user type summary data"""
    query = """
    SELECT 
        user_type,
        total_trips,
        avg_duration,
        min_duration,
        max_duration
    FROM user_type_summary
    WHERE user_type IS NOT NULL
    LIMIT 10
    """
    return safe_query_execution(query, query_name="user_type_summary")

def get_quick_stats():
    """Get quick stats from aggregated tables"""
    try:
        query = """
        SELECT 
            SUM(total_trips) as estimated_trips
        FROM user_type_summary
        """
        df = safe_query_execution(query, query_name="quick_stats")
        
        if not df.empty and df['estimated_trips'].iloc[0] is not None:
            return {'estimated_trips': int(df['estimated_trips'].iloc[0])}
        else:
            return {'estimated_trips': "N/A"}
        
    except Exception as e:
        return {'estimated_trips': "N/A"}

def create_station_trips_chart(df):
    """Create bar chart for top stations by total trips"""
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            showarrow=False,
            x=0.5, y=0.5,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Top 10 Stations by Total Trips", 
            height=400
        )
        return fig
    
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
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            showarrow=False,
            x=0.5, y=0.5,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Average Trip Duration by Hour", 
            height=400
        )
        return fig
    
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
    
    return fig

def create_monthly_stations_chart(df):
    """Create bar chart for top monthly stations"""
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            showarrow=False,
            x=0.5, y=0.5,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Top Monthly Stations", 
            height=400
        )
        return fig
    
    if 'year_month' in df.columns and not df.empty:
        latest_month = df['year_month'].iloc[0]
        recent_data = df[df['year_month'] == latest_month].head(10)
    else:
        recent_data = df.head(10)
        latest_month = "Latest"
    
    fig = px.bar(
        recent_data,
        x='trip_count',
        y='start_station_name',
        orientation='h',
        title=f'Top 10 Stations for {latest_month}',
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
    
    return fig

def create_user_type_chart(df):
    """Create bar chart for user type summary"""
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            showarrow=False,
            x=0.5, y=0.5,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="User Type Summary", 
            height=400
        )
        return fig
    
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
    
    # Show database info
    st.sidebar.info(f"📍 Original DB: {ORIGINAL_DB_PATH}")
    st.sidebar.info(f"🔒 Read-only copy: {READONLY_DB_PATH}")
    
    # Check if original database exists
    if not ORIGINAL_DB_PATH.exists():
        st.sidebar.error("❌ Original database not found")
        st.error("Original database file not found. Please ensure the ETL pipeline has completed.")
        return
    
    # Show original file info
    original_size = ORIGINAL_DB_PATH.stat().st_size / (1024*1024*1024)
    st.sidebar.info(f"💾 Original size: {original_size:.1f} GB")
    
    # Create read-only copy
    with st.spinner("Preparing database access..."):
        copy_success, copy_msg = create_readonly_copy()
    
    if not copy_success:
        st.sidebar.error(f"❌ {copy_msg}")
        st.error(f"Failed to create database copy: {copy_msg}")
        return
    
    st.sidebar.success(f"✅ {copy_msg}")
    
    # Show copy info
    if READONLY_DB_PATH.exists():
        copy_size = READONLY_DB_PATH.stat().st_size / (1024*1024*1024)
        st.sidebar.info(f"📋 Copy size: {copy_size:.1f} GB")
    
    # Test database connection
    with st.spinner("Testing database connection..."):
        connection_ok, connection_msg = test_database_connection()
    
    if connection_ok:
        st.sidebar.success(f"✅ {connection_msg}")
    else:
        st.sidebar.error(f"❌ {connection_msg}")
        st.error(f"Database connection failed: {connection_msg}")
        return
    
    # Get quick stats
    stats = get_quick_stats()
    if stats['estimated_trips'] != "N/A":
        st.sidebar.metric("Estimated Total Trips", f"{stats['estimated_trips']:,}")
    
    # Main content
    try:
        st.success("✅ Using read-only database copy to avoid conflicts!")
        
        with st.spinner("Loading dashboard data..."):
            # Load all data
            station_trips_df = load_trips_per_station_day()
            hourly_duration_df = load_avg_duration_by_hour()
            monthly_stations_df = load_top_stations_monthly()
            user_type_df = load_user_type_summary()
        
        # Check if we have any data
        data_available = {
            'station_trips': not station_trips_df.empty,
            'hourly_duration': not hourly_duration_df.empty,
            'monthly_stations': not monthly_stations_df.empty,
            'user_type': not user_type_df.empty
        }
        
        available_count = sum(data_available.values())
        
        if available_count == 0:
            st.warning("⚠️ No data found in any aggregated tables.")
            return
        
        st.info(f"📊 Loaded {available_count}/4 data sources successfully!")
        
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
        st.header("📊 Data Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            max_trips = station_trips_df['total_trips'].max() if not station_trips_df.empty else 0
            st.metric("Top Station Trips", f"{max_trips:,}" if max_trips > 0 else "N/A")
        
        with col2:
            max_duration = hourly_duration_df['avg_duration'].max() if not hourly_duration_df.empty else 0
            st.metric("Peak Hour Duration", f"{max_duration:.0f}s" if max_duration > 0 else "N/A")
        
        with col3:
            user_types_count = len(user_type_df) if not user_type_df.empty else 0
            st.metric("User Types", user_types_count)
        
        with col4:
            if not monthly_stations_df.empty and 'year_month' in monthly_stations_df.columns:
                latest_month = monthly_stations_df['year_month'].iloc[0]
            else:
                latest_month = "N/A"
            st.metric("Latest Month", latest_month)
        
        # Raw data expanders
        with st.expander("📋 View Raw Data"):
            tab1, tab2, tab3, tab4 = st.tabs(["Top Stations", "Hourly Patterns", "Monthly Stations", "User Types"])
            
            with tab1:
                if not station_trips_df.empty:
                    st.dataframe(station_trips_df, use_container_width=True)
                else:
                    st.write("No data available")
            
            with tab2:
                if not hourly_duration_df.empty:
                    st.dataframe(hourly_duration_df, use_container_width=True)
                else:
                    st.write("No data available")
            
            with tab3:
                if not monthly_stations_df.empty:
                    st.dataframe(monthly_stations_df, use_container_width=True)
                else:
                    st.write("No data available")
            
            with tab4:
                if not user_type_df.empty:
                    st.dataframe(user_type_df, use_container_width=True)
                else:
                    st.write("No data available")
            
    except Exception as e:
        st.error(f"❌ Error loading dashboard: {str(e)}")

if __name__ == "__main__":
    main()