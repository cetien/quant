설계 원칙 3가지

1. 레이어 간 단방향 의존성 UI는 analysis를 호출하고, analysis는 storage를 호출하고, storage는 ingestion 결과를 받습니다. 역방향 참조가 없으면 어느 레이어든 독립적으로 테스트할 수 있습니다.
2. factors/ 를 파일 단위로 분리한 이유 새 팩터를 추가할 때 기존 파일을 건드리지 않습니다. factors/ 안에 새 파일만 추가하고 scorer.py에 등록하면 끝입니다. Perplexity·Gemini 구조는 processor.py 하나에 모든 팩터를 @staticmethod로 쌓았는데, 팩터가 10개를 넘으면 관리가 어려워집니다.
3. UI components/ 공통화 캔들차트, 팩터 테이블은 scanner.py와 deep_dive.py 양쪽에서 씁니다. components/에 한 번 만들어두면 수정할 때 한 곳만 고칩니다.

UI는 Streamlit을 1순위로 권장합니다. 분석 코드와 같은 Python 파일 안에서 돌아가고, 데이터 테이블·차트·필터 슬라이더가 10~20줄로 구현됩니다. Dash는 커스터마이징이 필요할 때 차선입니다.
