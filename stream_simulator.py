import socket
import time
import random
from datetime import datetime

def generate_transaction():
    product_ids = ['P001', 'P002', 'P003', 'P004']
    product_id = random.choice(product_ids)
    quantity = random.randint(1, 10)
    price = round(random.uniform(5.0, 100.0), 2)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return f"{product_id},{quantity},{price},{timestamp}"

def start_streaming(host='localhost', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen(1)
        print(f"Streaming on {host}:{port}")
        conn, addr = s.accept()
        with conn:
            while True:
                transaction = generate_transaction()
                conn.sendall((transaction + '\n').encode('utf-8'))
                time.sleep(1)

if __name__ == "__main__":
    start_streaming()

