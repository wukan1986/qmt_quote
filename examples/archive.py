import sys
from datetime import datetime
from pathlib import Path

from loguru import logger
from npyt import NPYT

# 添加当前目录和上一级目录到sys.path
sys.path.insert(0, str(Path(__file__).parent))  # 当前目录
sys.path.insert(0, str(Path(__file__).parent.parent))  # 上一级目录

from config import FILE_d1t, BACKUP_DIR
from qmt_quote.inputs import generate_code

if __name__ == "__main__":
    print("=" * 60)
    print("1. 一定要在收盘后不再接收行情才能归档文件，否者继续记录行情失败")
    print("2. 归档前请关闭其他占用内存映射文件的程序")
    # 可以去除多余代码，方便定时运行归档脚本
    while True:
        code1 = generate_code(4)
        code2 = input(f"输入 `:q` 退出, 输入 `{code1}` 归档文件：")
        if code2 == ":q":
            break
        if code1 == code2:
            path = Path(BACKUP_DIR) / datetime.now().strftime("%Y%m%d")
            d1t = NPYT(FILE_d1t).load(mmap_mode="r")
            if d1t.resize():
                d1t.backup(path)
                break
            else:
                logger.error("归档失败!!!请关闭其他占用内存映射文件的程序后重试 {}", FILE_d1t)
                continue
