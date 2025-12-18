import streamlit as st
import pandas as pd
import plotly.express as px
import os
import datetime

# --- SESSION STATE INITIALIZATION ---
if 'rejected_movies' not in st.session_state:
    st.session_state.rejected_movies = []

if 'accepted_movie' not in st.session_state:
    st.session_state.accepted_movie = None

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Personal BI Dashboard", layout="wide")

# --- DATA LOADING FUNCTION ---
@st.cache_data
def load_data():
    path_prod = 'gold_layer/fact_daily_productivity.parquet'
    path_genre = 'gold_layer/fact_genre_stats.parquet'
    path_tmdb = 'silver_layer/dim_tmdb_movies.parquet'

    df_prod = pd.read_parquet(path_prod) if os.path.exists(path_prod) else None
    df_genre = pd.read_parquet(path_genre) if os.path.exists(path_genre) else None
    df_tmdb = pd.read_parquet(path_tmdb) if os.path.exists(path_tmdb) else None
    
    return df_prod, df_genre, df_tmdb

df_prod, df_genre, df_tmdb = load_data()

# --- HEADER ---
st.title("BI Dashboard: Personal Analytics")
st.markdown("""
This dashboard analyzes your life data through 4 distinct analytical lenses:
1. Report: What happened in the past? (Descriptive)
2. Diagnosis: What are my hidden habits and patterns? (Diagnostic)
3. Forecast: What will likely happen tomorrow? (Predictive)
4. Action: What specific steps should I take now? (Prescriptive)
""")

if df_prod is None:
    st.error("Data not found. Please ensure the ETL pipeline has been executed.")
    st.stop()

# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs([
    "1. Report (Descriptive)", 
    "2. Diagnosis (Diagnostic)", 
    "3. Forecast (Predictive)", 
    "4. Action (Prescriptive)"
])

# ==============================================================================
# 1. DESCRIPTIVE ANALYTICS (FOCUS: HISTORICAL DATA)
# ==============================================================================
with tab1:
    st.header("Performance Report")
    st.caption("A summary of your historical performance metrics.")
    
    col1, col2 = st.columns(2)
    total_jam = df_prod['total_hours'].sum()
    total_aktivitas = df_prod['total_activities'].sum()
    
    col1.metric("Total Productive Hours", f"{total_jam:.1f} Hours")
    col2.metric("Completed Activities", f"{total_aktivitas} Items")
    
    st.write("Daily Productivity Timeline:")
    fig_desc = px.line(df_prod, x='date', y='total_hours', markers=True, 
                       title="Daily Productivity Trend", line_shape='spline')
    st.plotly_chart(fig_desc, use_container_width=True)

# ==============================================================================
# 2. DIAGNOSTIC ANALYTICS (FOCUS: PATTERNS & BEHAVIOR)
# ==============================================================================
with tab2:
    st.header("Habit Diagnosis")
    st.caption("Analyzing underlying patterns in your work rhythm and personal interests.")
    
    # --- DATA PROCESSING ---
    df_prod['date'] = pd.to_datetime(df_prod['date'])
    df_prod['day_name'] = df_prod['date'].dt.day_name()
    
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_avg = df_prod.groupby('day_name')['total_hours'].mean().reindex(days_order).fillna(0).reset_index()
    best_day = daily_avg.loc[daily_avg['total_hours'].idxmax()]
    
    # --- COLUMN 1: RHYTHM ANALYSIS ---
    col_diag1, col_diag2 = st.columns(2)
    
    with col_diag1:
        st.subheader("Weekly Energy Rhythm")
        
        if best_day['day_name'] in ['Saturday', 'Sunday']:
            tipe_orang = "Weekend Warrior"
            pesan = "Unique! You are most productive when others are resting."
        elif best_day['day_name'] == 'Monday':
            tipe_orang = "Monday Starter"
            pesan = "Great, you start the week with maximum energy!"
        else:
            tipe_orang = "Mid-Week Grinder"
            pesan = "Your energy peaks in the middle of the week."
            
        st.markdown(f"Productivity Type: :blue[{tipe_orang}]")
        st.write(f"Data shows you are most productive on {best_day['day_name']} (Average {best_day['total_hours']:.1f} hours).")
        st.write(f"{pesan}")
        
        fig_rhythm = px.bar(daily_avg, x='day_name', y='total_hours', 
                            title="Average Productivity by Day",
                            color='total_hours', color_continuous_scale='Blues')
        st.plotly_chart(fig_rhythm, use_container_width=True)

    # --- COLUMN 2: INTEREST PROFILING ---
    with col_diag2:
        st.subheader("Interest Profile")
        
        # Category Audit
        top_cat = df_prod.groupby('category')['total_hours'].sum().sort_values(ascending=False).head(1)
        cat_name = top_cat.index[0]
        cat_val = top_cat.values[0]
        pct_val = (cat_val / total_jam) * 100
        
        st.markdown("Main Focus Area:")
        if pct_val > 50:
            st.warning(f"Imbalance Warning: {pct_val:.0f}% of your time is spent solely on {cat_name}. Consider diversifying your activities.")
        else:
            st.success(f"Balanced: Your top category ({cat_name}) consumes {pct_val:.0f}% of your time.")
            
        st.divider()
        
        # Viewer Personality
        st.markdown("Viewer Personality:")
        if df_genre is not None:
            top_genre_row = df_genre.sort_values('total_watched', ascending=False).iloc[0]
            fav_genre = top_genre_row['genre_name']
            
            julukan_map = {
                'Action': 'Adrenaline Junkie',
                'Comedy': 'Laugh Seeker',
                'Drama': 'The Melancholic',
                'Horror': 'The Brave',
                'Sci-Fi': 'Future Visionary',
                'Romance': 'True Romantic'
            }
            julukan = julukan_map.get(fav_genre, f"{fav_genre} Enthusiast")
            
            st.markdown(f"You are: {julukan}")
            st.write(f"The {fav_genre} genre dominates your watch history. This suggests you seek entertainment that is...")
            
            if fav_genre == 'Action': st.write("...fast-paced, intense, and stimulating.")
            elif fav_genre == 'Comedy': st.write("...lighthearted and stress-relieving.")
            elif fav_genre == 'Drama': st.write("...deep and emotionally engaging.")
            else: st.write(f"...specifically aligned with {fav_genre} themes.")
        else:
            st.write("Insufficient genre data for profiling.")

# ==============================================================================
# 3. PREDICTIVE ANALYTICS (FOCUS: FUTURE ESTIMATION)
# ==============================================================================
with tab3:
    st.header("Future Forecast")
    st.caption("Estimating tomorrow's output based on recent trends.")
    
    df_sorted = df_prod.sort_values('date', ascending=False)
    last_3_days = df_sorted.head(3)
    avg_recent = last_3_days['total_hours'].mean()
    last_val = df_sorted.iloc[0]['total_hours']
    
    prediction_status = ""
    if last_val > avg_recent:
        prediction_status = "RISING TREND"
        predicted_val = avg_recent * 1.1
        prediction_text = "You are in a positive momentum! Tomorrow's productivity is likely to remain high."
        status_color = "green"
    elif last_val < (avg_recent * 0.5):
        prediction_status = "SHARP DROP"
        predicted_val = avg_recent
        prediction_text = "Significant energy drop detected. The system predicts tomorrow will be a recovery phase (Rebound)."
        status_color = "red"
    else:
        prediction_status = "STABLE"
        predicted_val = avg_recent
        prediction_text = "Your rhythm is stable. Tomorrow is predicted to proceed normally like your daily average."
        status_color = "blue"

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        st.markdown(f"Trend Status: :{status_color}[{prediction_status}]")
        st.write(prediction_text)
    with col_p2:
        st.metric("Target for Tomorrow", f"{predicted_val:.1f} Hours")
        st.write(f"(Based on 3-day average: {avg_recent:.1f} hours)")

# ==============================================================================
# 4. PRESCRIPTIVE ANALYTICS (FOCUS: DECISIONS & ACTIONS)
# ==============================================================================
with tab4:
    st.header("Decision Center")
    st.caption("Actionable advice and entertainment permissions based on analysis.")

    col_act1, col_act2 = st.columns(2)
    current_status = prediction_status if 'prediction_status' in locals() else "STABLE"
    
    # --- LEFT: WORK STRATEGY ---
    with col_act1:
        st.subheader("Work Strategy")
        
        if current_status == "SHARP DROP":
            st.warning("Recovery Mode Activated")
            st.markdown("""
            1. Micro-Tasks: Do not take on large tasks. Break them down.
            2. 25-5 Technique: Focus for 25 mins, rest for 5 mins absolutely.
            3. Digital Detox: Avoid digital distractions for the first 2 hours.
            """)
        elif current_status == "RISING TREND":
            st.success("High-Performance Mode")
            st.markdown("""
            1. Eat the Frog: Do the hardest task first.
            2. Deep Work: Block 2 hours without interruption.
            3. Leverage Momentum: Do not stop until the target is met.
            """)
        else:
            st.info("Maintenance Mode")
            st.markdown("""
            1. Review Schedule: Check calendar for tomorrow.
            2. Organize: Tidy up files or emails.
            3. Consistency: Maintain regular working hours.
            """)
    
    # --- RIGHT: ENTERTAINMENT PERMISSION ---
    with col_act2:
        st.subheader("Entertainment Permission")
        
        show_movies = False
        
        if current_status == "SHARP DROP":
            st.error("DECISION: MOVIE FASTING")
            st.write("System detects performance drop. Watching movies now risks worsening procrastination.")
            st.markdown("Alternative Advice: Sleep early, read a physical book, or meditate.")
            
            if st.checkbox("I promise to watch only 1 movie (Override System)"):
                show_movies = True
                
        elif current_status == "RISING TREND":
            st.success("DECISION: REWARD GRANTED")
            st.write("You have been very productive! Please enjoy a quality movie as a reward.")
            show_movies = True
            
        else:
            st.success("DECISION: ALLOWED (MODERATE)")
            st.write("Watching is allowed to maintain mood, but limit duration to ensure sleep quality.")
            show_movies = True

        # --- MOVIE RECOMMENDATION DISPLAY ---
        if show_movies and df_tmdb is not None:
            st.divider()
            
            if current_status == "RISING TREND":
                filter_logic = (df_tmdb['popularity'] > 50)
                mood_title = "Trending Movies (Popular)"
            else:
                filter_logic = (df_tmdb['vote_average'] > 7.5)
                mood_title = "Top Rated Movies (Quality Time)"
            
            candidate_movies = df_tmdb[
                filter_logic & 
                (~df_tmdb['title'].isin(st.session_state.rejected_movies))
            ].sort_values('popularity', ascending=False)
            
            if not candidate_movies.empty:
                rec_movie = candidate_movies.iloc[0]
                judul = rec_movie['title']
                rating = rec_movie['vote_average']
                overview = rec_movie.get('overview', 'Summary not available.')
                
                st.markdown(f"{mood_title}")
                
                with st.container(border=True):
                    st.markdown(f"{judul}")
                    st.caption(f"TMDB Rating: {rating}/10 | Release: {rec_movie['release_date']}")
                    st.write(overview)
                    
                    st.write("")
                    c_btn1, c_btn2 = st.columns(2)
                    
                    if c_btn1.button("Accept & Watch", key="btn_acc", use_container_width=True):
                        st.session_state.accepted_movie = judul
                        st.balloons()
                        
                    if c_btn2.button("Change Movie", key="btn_rej", use_container_width=True):
                        st.session_state.rejected_movies.append(judul)
                        st.rerun()
                
                if st.session_state.accepted_movie == judul:
                    st.success(f"Enjoy watching {judul}! Don't forget to get back to work tomorrow!")
            else:
                st.info("Out of ideas! Try resetting the rejection list if you want to start over.")
                if st.button("Reset List"):
                    st.session_state.rejected_movies = []
                    st.rerun()