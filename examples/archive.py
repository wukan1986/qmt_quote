from config import FILE_1t, FILE_1m

from qmt_quote.memory_map import mmap_truncate
from qmt_quote.utils import generate_code

if __name__ == "__main__":
    print("注意：一定要在收盘后不再接收行情才能归档文件，否者继续记录行情失败")
    # 可以去除多余代码，方便定时运行归档脚本
    while True:
        code1 = generate_code(4)
        code2 = input(f"输入 `:q` 退出, 输入 `{code1}` 归档文件：")
        if code2 == ":q":
            break
        if code1 == code2:
            mmap_truncate(FILE_1t)
            mmap_truncate(FILE_1m)
            break
