import streamlit as st
from storage.db_manager import DuckDBManager

def render():
    st.title("📂 데이터 익스플로러")
    st.info("DuckDB 내의 모든 테이블과 데이터를 직접 확인합니다.")

    db = DuckDBManager()
    
    # 테이블 목록 가져오기
    tables_df = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
    
    if tables_df.empty:
        st.warning("조회 가능한 테이블이 없습니다.")
        db.close()
        return

    table_list = tables_df["table_name"].tolist()
    selected_table = st.selectbox("테이블 선택", table_list)

    # 데이터 조회
    if selected_table:
        row_count = db.row_count(selected_table)
        st.write(f"전체 행 수: `{row_count:,}`")
        
        limit = st.number_input("표시할 행 수", min_value=10, max_value=5000, value=100)
        
        df = db.query(f"SELECT * FROM {selected_table} LIMIT {limit}")
        st.dataframe(df, width="stretch")
        
        # 간단한 SQL 쿼리 실행기
        query = st.text_area("SQL 쿼리 직접 실행", f"SELECT * FROM {selected_table} LIMIT 10")
        if st.button("실행"):
            try:
                custom_df = db.query(query)
                st.write("결과:")
                st.dataframe(custom_df)
            except Exception as e:
                st.error(f"오류 발생: {e}")

    db.close()
