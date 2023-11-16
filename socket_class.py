import socket
import threading
from queue import Queue

class TCPSocketClass:
    def __init__(self,port,addr):
        self.port = port
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = {}
        self.recv_queue = Queue()
        self.lock = threading.Lock()
        server_thread = threading.Thread(target=self.socket_start)
        server_thread.start()
    
    def socket_start(self):
        try:
            self.sock.bind((self.addr,self.port))
            self.sock.listen(5)
            print("Listening on port " + str(self.port))
            while True:
                client_socket, client_addr = self.sock.accept()
                print(f"Accepted connection from {client_addr[0]}:{client_addr[1]}")
                t = threading.Thread(target=self.listener,args=(client_socket,))
                t.start()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.sock.close()

    def send(self,msg,send_addr,send_port):
        if send_addr not in self.connections:
            try:
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect((send_addr,send_port))
                self.connections[send_addr] = new_sock
            except Exception as e:
                return (2,f"Error: {e}")
        try:
            self.connections[send_addr].send(msg.encode())
        except Exception as e:
            return (1,f"Error: {e}")
        return (0,"Success")

    def listener(self,client_socket):
        try:
            while True:
                request = client_socket.recv(1024)
                request = request.decode()
                self.lock.acquire()
                self.recv_queue.put(request)
                self.lock.release()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            return

    def receive(self):
        self.lock.acquire()
        if self.recv_queue.empty():
            self.lock.release()
            return None
        else:
            msg = self.recv_queue.get()
            self.lock.release()
            return msg
    
    def ping(self,ping_addr,ping_port):
        sock_ping_test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock_ping_test.connect((ping_addr,ping_port))
            sock_ping_test.close()
            return True
        except Exception as e:
            sock_ping_test.close()
            return False