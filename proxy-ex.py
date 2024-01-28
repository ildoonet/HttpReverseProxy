from flask import Flask, request, jsonify
import queue
import threading
import time
import json
import uuid
import gzip
import base64
import logging

app = Flask(__name__)
requests_queue = queue.Queue()
responses = {}
condition = threading.Condition()


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


# save response for a request
@app.route('/12581958/response/<req_id>', methods=['POST'])
def handle_response(req_id):
    chunk_index = int(request.args['chunk_index'])
    total_chunks = int(request.args['total_chunks'])
    data = request.data

    with condition:
        if req_id not in responses or responses[req_id] is None:
            responses[req_id] = [None] * total_chunks
        responses[req_id][chunk_index] = data

        if all(chunk is not None for chunk in responses[req_id]):
            # 모든 조각을 하나로 결합
            full_data = b''.join(responses[req_id])

            decompressed_data = gzip.decompress(full_data)
            decompressed_string = decompressed_data.decode('utf-8')
            response_data = json.loads(decompressed_string)

            response_data['body'] = base64.b64decode(response_data['body'])

            responses[req_id] = response_data
            condition.notify_all()  # 응답 상태 변경 알림
    return '', 200

# return a request from a queue
@app.route('/12581958/queue')
def get_queue():
    queued_requests = []
    if not requests_queue.empty():
        req = requests_queue.get()
        queued_requests.append(req)
    return jsonify(queued_requests)

@app.route('/', methods=['GET'])
def enqueue_request_root():
    return enqueue_request('')

# request to queue
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def enqueue_request(path):
    data = request.json if request.method == 'POST' else {}
    headers = {k:v for k, v in request.headers.items()}
    req = {'path': path, 'method': request.method, 'data': data, 'headers': headers, 'params': request.args}
    req['request_id'] = str(uuid.uuid4())
    requests_queue.put(req)
    req_id = req['request_id']
    responses[req_id] = None  # 초기 응답 상태는 None

    # 요청 처리 대기
    with condition:
        while not isinstance(responses[req_id], dict):
            condition.wait()
        response = responses.pop(req_id)
        return app.response_class(
            response=response['body'],
            status=response['status_code'],
            mimetype=response.get('mimetype', 'application/json'),  # mimetype 사용
            content_type=response.get('mimetype', 'application/json')  # mimetype 사용
        )

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run the Flask application.')
    parser.add_argument('--port', type=int, default=8083, help='Port to run the Flask application on')
    
    # Parse the arguments
    args = parser.parse_args()
  
    app.run(host='0.0.0.0', port=args.port, threaded=True)
