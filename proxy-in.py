import argparse
import requests
import threading
import time
import json
import gzip
import base64


def process_requests(EXTERNAL_PROXY_SERVER, INTERNAL_WEB_SERVER, chunk_size):
    while True:
        # 외부 프록시 서버의 큐를 확인
        done = False
        response = requests.get(EXTERNAL_PROXY_SERVER + '/12581958/queue')
        if response.status_code // 100 == 2:
            for request_data in response.json():
                req_id = request_data['request_id']
                path = request_data['path']
                method = request_data['method']
                data = request_data.get('data')
                headers = request_data.get('headers', {})
                params = request_data.get('params', {})

                # 실제 내부 웹 서버로 요청 전달
                for _ in range(5):

                    try:
                        if method == 'GET':
                             resp = requests.get(f'{INTERNAL_WEB_SERVER}/{path}', headers=headers, params=params)
                        elif method == 'POST':
                             resp = requests.post(f'{INTERNAL_WEB_SERVER}/{path}', json=data, headers=headers, params=params)
                        elif method == 'PUT':
                             resp = requests.put(f'{INTERNAL_WEB_SERVER}/{path}', json=data, headers=headers, params=params)
                        elif method == 'DELETE':
                             resp = requests.delete(f'{INTERNAL_WEB_SERVER}/{path}', headers=headers, params=params)
                        else:
                             continue  # 또는 적절한 오류 처리
                        break
                    except Exception as e:
                        print(e)

                # 외부 프록시 서버에 전송할 응답 데이터 구성
                response_data = {
                    'status_code': resp.status_code,
                    'headers': dict(resp.headers),
                    'mimetype': resp.headers.get('Content-Type')
                }

                base64_encoded_data = base64.b64encode(resp.content).decode('utf-8')

                response_data['body'] = base64_encoded_data

                jsondata = json.dumps(response_data)
                byte_data = jsondata.encode('utf-8')

                # 데이터 압축
                compressed_data = gzip.compress(byte_data)

                total_size = len(compressed_data)

                chunks = [compressed_data[i:i+chunk_size] for i in range(0, total_size, chunk_size)]

                # 결과를 외부 프록시 서버에 전송
                for i, chunk in enumerate(chunks):
                    chunk_data = {
                       'chunk_index': i,
                       'total_chunks': len(chunks),
                    }

                    try:
                        requests.post(EXTERNAL_PROXY_SERVER + f'/12581958/response/{req_id}', data=chunk, params=chunk_data, timeout=30)
                    except Exception as e:
                        print(req_id, e)
                        pass


        if not done:
            time.sleep(0.1)  # 주기적으로 확인


if __name__ == '__main__':
    # argparse를 사용하여 커맨드 라인 인자 처리
    parser = argparse.ArgumentParser(description='Process some requests.')
    parser.add_argument('--num_threads', type=int, default=1, help='Number of threads to run')
    parser.add_argument('--server', type=str, default="http://127.0.0.1:8080" help='Internal web server URL')
    parser.add_argument('--external', type=str, required=True, help='External proxy server URL')
    parser.add_argument('--num_threads', type=int, default=1, help='Number of threads to run')
    parser.add_argument('--chunk_size', type=int, default=10 * 1024 * 1024, help='Chunk size in bytes, default is 10MB')
    args = parser.parse_args()

    # Start threads with the provided arguments
    threads = []
    for _ in range(args.num_threads):
        thread = threading.Thread(target=process_requests, args=(args.external, args.server, args.chunk_size))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()
