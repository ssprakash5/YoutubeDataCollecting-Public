import streamlit as st
import mysql.connector
import pandas as pd

# Connecting to MySQL
def connect_to_mysql():
    db_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        port='3306',
        password='Surya123',
        database='youtube'
    )
    return db_connection

# Execute a SQL query and return results as a DataFrame
def execute_query(query):
    db_connection = connect_to_mysql()
    cursor = db_connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    db_connection.close()
    return pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])

# Define Streamlit app
def main():
    st.title("YouTube Data Analysis")

    # Query buttons
    query1_button = st.button("Query 1: Names of Videos and Their Corresponding Channels")
    query2_button = st.button("Query 2: Channels with Most Videos")
    query3_button = st.button("Query 3: Top 10 Most Viewed Videos")
    query4_button = st.button("Query 4: Comments on Videos")
    query5_button = st.button("Query 5: Videos with Most Likes")
    query6_button = st.button("Query 6: Total Likes and Dislikes for Videos")
    query7_button = st.button("Query 7: Total Views for Channels")
    query8_button = st.button("Query 8: Channels Publishing in 2022")
    query9_button = st.button("Query 9: Average Video Duration by Channel")
    query10_button = st.button("Query 10: Videos with Most Comments")

    if query1_button:
        query1 = """
        SELECT video_name, channel_name FROM combined_data;
        """
        result1 = execute_query(query1)
        st.header("Query 1: Names of Videos and Their Corresponding Channels")
        st.dataframe(result1)

    if query2_button:
        query2 = """
        SELECT channel_name, MAX(total_video_count) AS max_video_count
        FROM channels
        GROUP BY channel_name
        ORDER BY max_video_count DESC;
        """
        result2 = execute_query(query2)
        st.header("Query 2: Channels with Most Videos")
        st.dataframe(result2)

    if query3_button:
        query3 = """
        SELECT channel_name, video_name, MAX(view_count) AS max_view_count
        FROM combined_data
        GROUP BY channel_name, video_name
        ORDER BY max_view_count DESC
        LIMIT 10;
        """
        result3 = execute_query(query3)
        st.header("Query 3: Top 10 Most Viewed Videos")
        st.dataframe(result3)

    if query4_button:
        query4 = """
        SELECT video_name, like_count, dislike_count FROM combined_data;
        """
        result4 = execute_query(query4)
        st.header("Query 4: Comments on Videos")
        st.dataframe(result4)

    if query5_button:
        query5 = """
        SELECT channel_name, video_name, MAX(like_count) AS max_like_count
        FROM combined_data
        GROUP BY channel_name, video_name
        ORDER BY max_like_count DESC;
        """
        result5 = execute_query(query5)
        st.header("Query 5: Videos with Most Likes")
        st.dataframe(result5)

    if query6_button:
        query6 = """
        SELECT video_name, SUM(like_count) AS total_likes, SUM(dislike_count) AS total_dislikes
        FROM combined_data
        GROUP BY video_name;
        """
        result6 = execute_query(query6)
        st.header("Query 6: Total Likes and Dislikes for Videos")
        st.dataframe(result6)

    if query7_button:
        query7 = """
        SELECT channel_name, SUM(channel_views) AS total_views
        FROM channels
        GROUP BY channel_name;
        """
        result7 = execute_query(query7)
        st.header("Query 7: Total Views for Channels")
        st.dataframe(result7)

    if query8_button:
        query8 = """
        SELECT DISTINCT channel_name
        FROM combined_data
        WHERE YEAR(published_date) = 2022;
        """
        result8 = execute_query(query8)
        st.header("Query 8: Channels Publishing in 2022")
        st.dataframe(result8)

    if query9_button:
        query9 = """
        SELECT channel_name, AVG(duration) AS avg_duration
        FROM combined_data
        GROUP BY channel_name;
        """
        result9 = execute_query(query9)
        st.header("Query 9: Average Video Duration by Channel")
        st.dataframe(result9)

    if query10_button:
        query10 = """
        SELECT channel_name, video_name, MAX(comment_count) AS max_comment_count
        FROM combined_data
        GROUP BY channel_name, video_name
        ORDER BY max_comment_count DESC;
        """
        result10 = execute_query(query10)
        st.header("Query 10: Videos with Most Comments")
        st.dataframe(result10)

if __name__ == "__main__":
    main()
