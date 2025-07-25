import queue
import random
import string
import threading
from typing import Optional


def generate_code(length: int = 4) -> str:
    """生成验证码"""
    return ''.join(random.sample(string.digits, k=length))


def input_with_timeout(prompt: str, timeout: int = 10) -> Optional[str]:
    """带有超时的用户输入函数"""
    print(prompt, end='', flush=True)
    user_input = queue.Queue()

    def get_input():
        try:
            text = input()
            user_input.put(text)
        except:
            user_input.put(None)

    # 创建输入线程
    input_thread = threading.Thread(target=get_input)
    input_thread.daemon = True
    input_thread.start()

    # 等待输入或超时
    try:
        result = user_input.get(timeout=timeout)
        return result
    except queue.Empty:
        print()
        return None
