import streamlit as st
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="Manager Scouting Pro",
    page_icon="👔",
    layout="wide"
)

st.title("👔 Manager Scouting Pro")
st.markdown("Identify coaches with a specific tactical DNA. Select a reference manager to find their tactical twins.")

# --- 2. DATA LOADING & JOINING ---
conn = st.connection("snowflake")

@st.cache_data(ttl=3600)
def load_data():
    # 1. Fetch Raw Stats (The Tactical Profile) - IT ALREADY HAS MANAGER NAMES!
    stats_query = """
    SELECT * FROM PROD_WH_MARTS.TEAM_STYLES_FINAL
    WHERE MATCHES_PLAYED >= 5
    """
    df_stats = conn.query(stats_query)
    
    #  Ensure STATS data is present and columns are correct
    df_stats.columns = [c.upper() for c in df_stats.columns]
    if df_stats.empty:
        st.error("The primary stats table (`PROD_WH_MARTS.TEAM_STYLES_FINAL`) returned zero rows. Cannot proceed.")
        st.stop()
    
    # Check if MANAGER_NAME already exists in the stats table
    if 'MANAGER_NAME' not in df_stats.columns:
        st.error("MANAGER_NAME column not found in TEAM_STYLES_FINAL table.")
        st.error(f"Available columns: {list(df_stats.columns)}")
        st.stop()
    
    # Clean up manager names - remove any nulls
    df_stats['MANAGER_NAME'] = df_stats['MANAGER_NAME'].fillna(
        df_stats['TEAM_NAME'] + " Manager"
    )
    
    required_stats_cols = ['TEAM_NAME', 'SEASON', 'MANAGER_NAME']
    if not all(col in df_stats.columns for col in required_stats_cols):
        st.error(f"Missing critical columns in stats data. Required: {required_stats_cols}")
        st.stop()
    
    # Create a unique ID for the season profile
    df_stats['PROFILE_ID'] = (
        df_stats['MANAGER_NAME'] + " (" + 
        df_stats['TEAM_NAME'] + " " + 
        df_stats['SEASON'] + ")"
    )
    
    # Prepare Scaling
    features = [
        'AVG_POSSESSION', 'AVG_PASSES', 'AVG_SHOTS', 'AVG_XG',
        'PRESSING_INTENSITY', 'AVG_SHOT_QUALITY', 'TEAM_WIDTH'
    ]
    
    # Ensure features exist and handle nulls
    if not all(f in df_stats.columns for f in features):
         missing = [f for f in features if f not in df_stats.columns]
         st.error(f"Missing required feature columns: {missing}. Check PROD_WH_MARTS.TEAM_STYLES_FINAL.")
         st.stop()

    X = df_stats[features].fillna(0)
    
    if X.shape[0] == 0:
        st.error("The data is empty. Cannot run analysis.")
        st.stop()
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    df_stats['SCALED_FEATURES'] = X_scaled.tolist()
    
    return df_stats, features, scaler

# --- INITIAL DATA LOAD ---
try:
    df, features, scaler = load_data()
except Exception as e:
    st.error(f"🔴 Fatal Data Load Error: {e}")
    st.error("Please check your Snowflake connection and table structure.")
    st.stop()

# --- 3. SIMILARITY ENGINE ---
def find_similar_managers(target_vector, df_candidates, n=5):
    target_array = np.array(target_vector)
    
    # Calculate Distance using Euclidean norm
    df_candidates['DISTANCE'] = df_candidates['SCALED_FEATURES'].apply(
        lambda x: np.linalg.norm(target_array - np.array(x))
    )
    
    # Sort by similarity (smallest distance is most similar)
    return df_candidates.sort_values('DISTANCE').head(n)

# --- 4. UI & LOGIC ---

with st.sidebar:
    st.header("Scouting Filters")
    
    df_filtered = df.copy() 
    
    # Check for empty data
    if df_filtered.empty:
        st.warning(f"No data available for analysis.")
        st.stop()
        
    # B. Select Reference Manager
    # Filter out any null or empty manager names
    valid_managers_df = df_filtered[
        df_filtered['MANAGER_NAME'].notna() & 
        (df_filtered['MANAGER_NAME'].str.strip() != '') &
        ~df_filtered['MANAGER_NAME'].str.endswith(' Manager')
    ].copy()
    
    # If filtering removes everything, use all data
    if valid_managers_df.empty:
        st.warning("⚠️ No distinct manager names found, showing all profiles")
        valid_managers_df = df_filtered.copy()
    
    # Get unique manager names and sort
    known_managers = sorted(valid_managers_df['MANAGER_NAME'].unique())
    
    if not known_managers:
        st.error("No manager profiles found in the data.")
        st.stop()
    
    selected_manager_name = st.selectbox(
        "Select Reference Manager:", 
        known_managers,
        help=f"Showing {len(known_managers)} managers"
    )
    
    # C. Select Specific Season (Tactical Version)
    manager_seasons = df_filtered[df_filtered['MANAGER_NAME'] == selected_manager_name].copy()
    
    # Create better display labels (with safe handling if MATCHES_PLAYED doesn't exist)
    if 'MATCHES_PLAYED' in manager_seasons.columns:
        manager_seasons['DISPLAY_LABEL'] = (
            manager_seasons['SEASON'] + " @ " + 
            manager_seasons['TEAM_NAME'] + 
            " (" + manager_seasons['MATCHES_PLAYED'].astype(str) + " matches)"
        )
    else:
        manager_seasons['DISPLAY_LABEL'] = (
            manager_seasons['SEASON'] + " @ " + 
            manager_seasons['TEAM_NAME']
        )
    
    season_options = manager_seasons.set_index('DISPLAY_LABEL')['PROFILE_ID'].to_dict()
    
    selected_display = st.selectbox(
        "Select Tactical Version (Season):", 
        list(season_options.keys())
    )
    selected_profile_id = season_options[selected_display]
    
    find_btn = st.button("Find Similar Coaches", type="primary")

# --- MAIN APPLICATION RESULTS ---
if find_btn:
    # 1. Get Target Profile
    try:
        target_row = df_filtered[df_filtered['PROFILE_ID'] == selected_profile_id].iloc[0]
        target_vec = target_row['SCALED_FEATURES']
    except IndexError:
        st.error("Could not find the selected profile data. Please try another selection.")
        st.stop()
        
    # 2. Define Candidates (Filter out the selected manager himself)
    candidates = df_filtered[df_filtered['MANAGER_NAME'] != selected_manager_name].copy()
    
    # 3. Run Algorithm
    results = find_similar_managers(target_vec, candidates, n=10)
    
    # 4. Deduplicate (Keep the best match for each unique manager name)
    results_deduped = results.drop_duplicates(subset=['MANAGER_NAME'], keep='first').head(5)
    
    # --- DISPLAY RESULTS ---
    st.header(f"Tactical Twins: {selected_manager_name}")
    st.caption(f"Based on analysis of **{target_row['TEAM_NAME']} ({target_row['SEASON']})** | Style: **{target_row.get('STYLE_NAME', 'Unknown')}**")
    
    # Key Metrics Comparison
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Possession", f"{target_row['AVG_POSSESSION']:.1f}%")
    col2.metric("Pressing Intensity", f"{target_row['PRESSING_INTENSITY']:.2f}")
    col3.metric("Passing Tempo", f"{target_row['AVG_PASSES']:.0f}")
    col4.metric("xG Created", f"{target_row['AVG_XG']:.2f}")
    
    st.divider()
    st.subheader("Top 5 Most Similar Managers")
    
    for idx, (_, row) in enumerate(results_deduped.iterrows()):
        # Calculate a simple similarity score for display
        similarity_score = (1 / (1 + row['DISTANCE'])) * 100
        
        with st.expander(f"#{idx+1} {row['MANAGER_NAME']} ({row['TEAM_NAME']})", expanded=(idx < 2)):
            c1, c2, c3 = st.columns([1, 2, 2])
            
            with c1:
                st.metric("Similarity Score", f"{similarity_score:.1f}%")
                st.caption(f"Season: {row['SEASON']}")
                
            with c2:
                # Small comparison dataframe
                comp_data = pd.DataFrame({
                    'Metric': ['Possession %', 'Pressing Intensity', 'Avg xG'],
                    'Target': [
                        f"{target_row['AVG_POSSESSION']:.1f}",
                        f"{target_row['PRESSING_INTENSITY']:.2f}",
                        f"{target_row['AVG_XG']:.2f}"
                    ],
                    'Match': [
                        f"{row['AVG_POSSESSION']:.1f}",
                        f"{row['PRESSING_INTENSITY']:.2f}",
                        f"{row['AVG_XG']:.2f}"
                    ]
                })
                
                st.dataframe(comp_data.set_index('Metric'), use_container_width=True)
            
            with c3:
                # Tactical DNA Chart (Using normalized/scaled features for comparison)
                target_scaled = scaler.transform(target_row[features].to_frame().T)[0]
                match_scaled = row['SCALED_FEATURES']
                
                chart_df = pd.DataFrame({
                    'Feature': ['Possession', 'Passes', 'Shots', 'xG', 'Pressing', 'Shot Quality', 'Width'],
                    'Target': target_scaled,
                    'Match': match_scaled
                }).set_index('Feature')
                
                st.bar_chart(chart_df, color=["#FF4B4B", "#1C83E1"])

    # 5. Full Comparison Chart
    st.subheader("Detailed Comparison Matrix (Normalized Features)")
    
    # Prepare data for big chart
    all_profiles = pd.concat([target_row.to_frame().T, results_deduped])
    chart_data = all_profiles[['MANAGER_NAME'] + features].set_index('MANAGER_NAME')
    
    # Normalize for visualization
    chart_data_norm = pd.DataFrame(
        scaler.transform(chart_data[features]), 
        columns=[c.replace('AVG_', '').replace('_', ' ').title() for c in features],
        index=chart_data.index
    )
    st.line_chart(chart_data_norm.T)