#!/usr/bin/env python3
"""
HTTP/HTTPS代理转发服务器
用于解决网络连接问题，转发外网请求
"""

import socket
import threading
import argparse
import logging
import select
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProxyServer:
    def __init__(self, listen_port=8888, upstream_proxy=None, upstream_port=7890):
        self.listen_port = listen_port
        self.upstream_proxy = upstream_proxy
        self.upstream_port = upstream_port
        self.server_socket = None
        
    def start(self):
        """启动代理服务器"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('127.0.0.1', self.listen_port))
            self.server_socket.listen(128)
            logger.info(f"代理服务器启动成功，监听端口: {self.listen_port}")
            logger.info(f"上游代理: {self.upstream_proxy}:{self.upstream_port if self.upstream_proxy else '无'}")
            
            while True:
                client_socket, client_addr = self.server_socket.accept()
                logger.info(f"收到连接: {client_addr}")
                thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                thread.daemon = True
                thread.start()
        except Exception as e:
            logger.error(f"服务器启动失败: {e}")
            raise
        
    def handle_client(self, client_socket):
        """处理客户端请求"""
        try:
            # 接收HTTP请求
            request = client_socket.recv(4096)
            if not request:
                client_socket.close()
                return
                
            # 解析请求行
            lines = request.decode('utf-8', errors='ignore').split('\r\n')
            if not lines:
                client_socket.close()
                return
                
            request_line = lines[0]
            logger.info(f"请求: {request_line}")
            
            # 判断是HTTP还是HTTPS
            if request_line.startswith('CONNECT'):
                # HTTPS请求
                self.handle_https(client_socket, request)
            else:
                # HTTP请求
                self.handle_http(client_socket, request)
                
        except Exception as e:
            logger.error(f"处理客户端请求失败: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            
    def handle_http(self, client_socket, request):
        """处理HTTP请求"""
        try:
            if self.upstream_proxy:
                # 通过上游代理转发
                target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                target_socket.settimeout(30)
                target_socket.connect((self.upstream_proxy, self.upstream_port))
                
                # 转发请求到上游代理
                target_socket.sendall(request)
                
                # 接收响应并返回给客户端
                while True:
                    response = target_socket.recv(4096)
                    if not response:
                        break
                    client_socket.sendall(response)
                    
                target_socket.close()
            else:
                # 直接连接目标服务器
                lines = request.decode('utf-8', errors='ignore').split('\r\n')
                if lines:
                    request_line = lines[0]
                    parts = request_line.split(' ')
                    if len(parts) >= 2:
                        url = parts[1]
                        if url.startswith('http://'):
                            url = url[7:]
                        
                        host_port = url.split('/')[0]
                        if ':' in host_port:
                            host, port = host_port.split(':')
                            port = int(port)
                        else:
                            host = host_port
                            port = 80
                            
                        # 连接目标服务器
                        target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        target_socket.settimeout(30)
                        target_socket.connect((host, port))
                        
                        # 发送请求
                        target_socket.sendall(request)
                        
                        # 接收响应并返回给客户端
                        while True:
                            response = target_socket.recv(4096)
                            if not response:
                                break
                            client_socket.sendall(response)
                            
                        target_socket.close()
                        
        except Exception as e:
            logger.error(f"处理HTTP请求失败: {e}")
        finally:
            client_socket.close()
            
    def handle_https(self, client_socket, request):
        """处理HTTPS请求（CONNECT方法）"""
        try:
            lines = request.decode('utf-8', errors='ignore').split('\r\n')
            if not lines:
                return
                
            # 解析CONNECT请求
            request_line = lines[0]
            parts = request_line.split(' ')
            if len(parts) < 2:
                return
                
            host_port = parts[1]
            if ':' in host_port:
                host, port = host_port.split(':')
                port = int(port)
            else:
                host = host_port
                port = 443
                
            logger.info(f"HTTPS连接: {host}:{port}")
            
            if self.upstream_proxy:
                # 通过上游代理建立CONNECT隧道
                target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                target_socket.settimeout(30)
                target_socket.connect((self.upstream_proxy, self.upstream_port))
                
                # 向上游代理发送CONNECT请求
                target_socket.sendall(request)
                
                # 接收上游代理响应
                response = target_socket.recv(4096)
                client_socket.sendall(response)
                
                if b'200' in response:
                    # 隧道建立成功，开始转发数据
                    self.forward_data(client_socket, target_socket)
                    
                target_socket.close()
            else:
                # 直接连接目标服务器
                target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                target_socket.settimeout(30)
                target_socket.connect((host, port))
                
                # 返回200 Connection Established
                client_socket.sendall(b'HTTP/1.1 200 Connection Established\r\n\r\n')
                
                # 隧道建立成功，开始转发数据
                self.forward_data(client_socket, target_socket)
                
                target_socket.close()
                
        except Exception as e:
            logger.error(f"处理HTTPS请求失败: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            
    def forward_data(self, sock1, sock2):
        """转发两个socket之间的数据"""
        sockets = [sock1, sock2]
        try:
            while True:
                readable, _, errors = select.select(sockets, [], sockets, 30)
                
                if errors:
                    break
                    
                if not readable:
                    continue
                    
                for sock in readable:
                    data = sock.recv(4096)
                    if not data:
                        return
                    
                    other = sock2 if sock is sock1 else sock1
                    other.sendall(data)
                    
        except Exception as e:
            logger.error(f"数据转发失败: {e}")
            
    def stop(self):
        """停止代理服务器"""
        if self.server_socket:
            self.server_socket.close()
            logger.info("代理服务器已停止")

def main():
    parser = argparse.ArgumentParser(description='HTTP/HTTPS代理转发服务器')
    parser.add_argument('-l', '--listen', type=int, default=8888, help='监听端口 (默认: 8888)')
    parser.add_argument('-p', '--proxy', type=str, default=None, help='上游代理地址')
    parser.add_argument('-t', '--tport', type=int, default=7890, help='上游代理端口 (默认: 7890)')
    parser.add_argument('-v', '--verbose', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    server = ProxyServer(
        listen_port=args.listen,
        upstream_proxy=args.proxy,
        upstream_port=args.tport
    )
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在停止服务器...")
        server.stop()

if __name__ == '__main__':
    main()
