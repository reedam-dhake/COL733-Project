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
        server_thread = threading.Thread(target=self.sock_start)
        server_thread.start()
    
    def socket_start(self):
        try:
            self.sock.bind((self.addr,self.port))
            self.sock.listen(5)
            print("Listening on port " + str(self.port))
            while True:
                client_socket, client_addr = self.sock.accept()
                print(f"Accepted connection from {client_addr[0]}:{client_addr[1]}")
                t = threading.Thread(target=self.listener,args=(client_socket))
                t.start()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.sock.close()

    def send(self,msg,send_port,send_addr):
        if send_addr not in self.connections:
            try:
                self.sock.connect((send_addr,send_port))
                self.connections.add(send_addr)
            except Exception as e:
                return (2,f"Error: {e}")
        try:
            self.sock.send(msg.encode())
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
