# 1. 변경된 파일 확인 (현재 상태 파악)
git status

# 2. 변경 사항을 스테이징 영역(Staging Area)에 추가
# 특정 파일만 추가: git add 파일명.py
# 전체 변경 사항 추가:
git add .

# 3. 로컬 저장소에 커밋 (기록 생성)
# -m 뒤에는 변경 내용을 요약한 메시지를 작성합니다.
git commit -m "feat: 가격 수집 로직 내 거래정지 종목 필터링 추가"

# 4. GitHub(원격 저장소)로 코드 전송
# 보통 기본 브랜치 명은 'main' 또는 'master'입니다.
git push origin main


> feat: 새로운 기능 추가
> fix: 버그 수정
> docs: 문서 수정
> style: 코드 포맷팅 (코드 변경 X)

# run
streamlit run ui/app.py
