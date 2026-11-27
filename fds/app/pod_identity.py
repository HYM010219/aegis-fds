"""시험할 때만 게이트웨이 응답에 처리 파드 이름을 붙인다.

    POD_IDENTITY_HEADER=1일 때 동작하며 기본값은 비활성화다. UID는 보내지 않는다.
"""
import os
import socket


class PodIdentityMiddleware:
    def __init__(self, app, pod_name=None, enabled=None):
        self.app = app
        self.enabled = os.getenv('POD_IDENTITY_HEADER', '0') == '1' if enabled is None else enabled
        name = pod_name or os.getenv('POD_NAME') or socket.gethostname()
        self.name = name.replace('\r', '?').replace('\n', '?').encode('ascii', errors='replace')

    async def __call__(self, scope, receive, send):
        if not self.enabled or scope['type'] != 'http':
            return await self.app(scope, receive, send)

        async def identified_send(message):
            if message['type'] == 'http.response.start':
                headers = [(k, v) for k, v in message.get('headers', [])
                           if k.lower() not in (b'x-gateway-pod', b'x-gateway-pod-uid')]
                headers.append((b'x-gateway-pod', self.name))
                message = {**message, 'headers': headers}
            await send(message)

        await self.app(scope, receive, identified_send)
