"""
common/logger.py
프로젝트 공통 로거 설정
"""
import logging
import sys
from pathlib import Path

LOG_DIR  = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "quant.log"

_FMT = "[%(asctime)s] [%(levelname)-8s] %(name)s - %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    모듈별 로거 반환. 중복 핸들러 방지.

    Usage:
        from common.logger import get_logger
        log = get_logger(__name__)
        log.info("started")
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(_FMT, datefmt=_DATE_FMT)

    # 콘솔 핸들러
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 파일 핸들러
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
