import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from database import init_db, insert_response, get_all_responses, get_department_avg

init_db()

st.set_page_config(
    page_title="Employee Engagement Survey System",
    page_icon="📊",
    layout="wide"
)

st.sidebar.title("📊 Survey System")
page = st.sidebar.radio("Navigate", ["📝 Submit Survey", "📈 Dashboard", "📋 All Responses", "📥 Export Data", "🔍 Data Quality", "⚙️ ETL Pipeline"])

if page == "📝 Submit Survey":
    st.title("📝 Employee Engagement Survey")
    st.markdown("Please fill out the survey honestly. All responses are confidential.")

    with st.form("survey_form"):
        st.subheader("Personal Details")
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Employee Name")
        with col2:
            dept = st.selectbox("Department", ["HR", "Engineering", "Sales", "Marketing", "Finance", "Operations"])
        with col3:
            role = st.selectbox("Role Level", ["Junior", "Mid-Level", "Senior", "Manager", "Director"])

        st.subheader("Rate the following (1 = Very Poor, 5 = Excellent)")
        col1, col2 = st.columns(2)
        with col1:
            wlb = st.slider("Work-Life Balance", 1, 5, 3)
            js  = st.slider("Job Satisfaction", 1, 5, 3)
            tc  = st.slider("Team Collaboration", 1, 5, 3)
        with col2:
            ms  = st.slider("Management Support", 1, 5, 3)
            cg  = st.slider("Career Growth Opportunities", 1, 5, 3)
            oe  = st.slider("Overall Engagement", 1, 5, 3)

        comments = st.text_area("Additional Comments (Optional)")
        submitted = st.form_submit_button("✅ Submit Survey")

        if submitted:
            if not name:
                st.error("Please enter your name.")
            else:
                insert_response({
                    "employee_name": name, "department": dept, "role": role,
                    "work_life_balance": wlb, "job_satisfaction": js,
                    "team_collaboration": tc, "management_support": ms,
                    "career_growth": cg, "overall_engagement": oe,
                    "comments": comments
                })
                st.success(f"✅ Thank you {name}! Your response has been recorded.")
                st.balloons()

elif page == "📈 Dashboard":
    st.title("📈 Engagement Analytics Dashboard")
    df = get_all_responses()

    if df.empty:
        st.warning("No responses yet. Submit some surveys first!")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Responses", len(df))
        col2.metric("Avg Overall Engagement", f"{df['overall_engagement'].mean():.2f} / 5")
        col3.metric("Avg Job Satisfaction", f"{df['job_satisfaction'].mean():.2f} / 5")
        col4.metric("Departments Covered", df['department'].nunique())

        st.markdown("---")

        dept_df = get_department_avg()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Department-wise Overall Engagement")
            fig = px.bar(dept_df, x='department', y='Overall_Engagement',
                         color='Overall_Engagement', color_continuous_scale='Blues',
                         range_y=[0, 5])
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Survey Responses by Department")
            dept_count = df['department'].value_counts().reset_index()
            dept_count.columns = ['Department', 'Count']
            fig2 = px.pie(dept_count, values='Count', names='Department', hole=0.4)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Average Scores Across All Metrics")
        metrics = ['work_life_balance', 'job_satisfaction', 'team_collaboration',
                   'management_support', 'career_growth', 'overall_engagement']
        labels = ['Work-Life Balance', 'Job Satisfaction', 'Team Collaboration',
                  'Management Support', 'Career Growth', 'Overall Engagement']
        values = [df[m].mean() for m in metrics]

        fig3 = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]],
            theta=labels + [labels[0]],
            fill='toself',
            line_color='royalblue'
        ))
        fig3.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 5])))
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Engagement Trend Over Time")
        df['submitted_at'] = pd.to_datetime(df['submitted_at'])
        df_sorted = df.sort_values('submitted_at')
        fig4 = px.line(df_sorted, x='submitted_at', y='overall_engagement',
                       title='Overall Engagement Over Time', markers=True)
        st.plotly_chart(fig4, use_container_width=True)

elif page == "📋 All Responses":
    st.title("📋 All Survey Responses")
    df = get_all_responses()
    if df.empty:
        st.warning("No responses yet.")
    else:
        dept_filter = st.multiselect("Filter by Department", options=df['department'].unique(), default=list(df['department'].unique()))
        filtered = df[df['department'].isin(dept_filter)]
        st.dataframe(filtered, use_container_width=True)
        st.info(f"Showing {len(filtered)} of {len(df)} responses")

elif page == "📥 Export Data":
    st.title("📥 Export Survey Data")
    df = get_all_responses()
    if df.empty:
        st.warning("No data to export.")
    else:
        st.success(f"Ready to export {len(df)} responses.")
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("⬇️ Download as CSV", data=csv, file_name="survey_data.csv", mime="text/csv")

elif page == "🔍 Data Quality":
    st.title("🔍 Data Quality Checks")
    from data_quality import run_quality_checks, get_summary_stats
    df_raw = get_all_responses()

    if df_raw.empty:
        st.warning("No data yet. Submit some surveys first!")
    else:
        st.subheader("Automated Quality Check Results")
        results, df = run_quality_checks()

        for _, row in results.iterrows():
            if row['status'] == "PASS":
                st.success(f"✅ {row['check']} — {row['details']}")
            elif row['status'] == "WARNING":
                st.warning(f"⚠️ {row['check']} — {row['details']}")
            else:
                st.error(f"❌ {row['check']} — {row['details']}")

        st.markdown("---")
        st.subheader("Statistical Summary of All Scores")
        stats = get_summary_stats(df_raw)
        st.dataframe(stats, use_container_width=True)

elif page == "⚙️ ETL Pipeline":
    st.title("⚙️ ETL Pipeline")
    from etl_pipeline import run_etl
    import os

    st.markdown("This page runs the full ETL pipeline on current survey data.")
    st.info("Pipeline: Extract from DB → Transform & Clean → Load to processed table → Export CSV")

    if st.button("▶️ Run ETL Pipeline Now"):
        df = get_all_responses()
        if df.empty:
            st.warning("No data to process!")
        else:
            df.to_csv("survey_export.csv", index=False)
            with st.spinner("Running ETL pipeline..."):
                result = run_etl("survey_export.csv")
            if result is not None:
                st.success("✅ ETL Pipeline completed successfully!")
                st.subheader("Transformed Data Preview")
                st.dataframe(result.head(10), use_container_width=True)

                if os.path.exists("etl_log.txt"):
                    st.subheader("ETL Log")
                    with open("etl_log.txt", "r") as f:
                        log_content = f.read()
                    st.code(log_content)
                    