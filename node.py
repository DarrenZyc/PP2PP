import threading
import sys
import signal
import socket
import time

port_range = range(50001, 50010)
localhost = "127.0.0.1"


class Node:

    def __init__(self, id):
        self.id = int(id)
        self.port = 50000 + self.id
        self.addr = (localhost, self.id + 50000)
        self.threads = list()
        self.succ = -1
        self.pred = -1

        t1 = threading.Thread(target=self.console)
        self.threads.append(t1)
        t1.start()

        t2 = threading.Thread(target=self.tcp_listener)
        t2.daemon = True
        self.threads.append(t2)
        t2.start()

        t3 = threading.Thread(target=self.search_listener)
        t3.daemon = True
        self.threads.append(t3)
        t3.start()

    def console(self):
        while True:
            command = input("> ")
            if command == "":
                continue
            command = command.split()
            if command[0] == "quit" or command[0] == "exit":
                print("Quitting ...")
                sys.exit(0)
            elif command[0] == "usage":
                print("quit \n transfer \n")
            elif command[0] == "transfer":
                print("Transfer ...")
            elif command[0] == "search":
                self.search_broadcast()
            elif command[0] == "message":
                flood_mode = False
                direct_msg = False
                if "-f" in command:
                    flood_mode = True
                if "-d" in command:
                    direct_msg = True
                if direct_msg:
                    self.direct_message(int(command[1]), str(command[-1]))
                if flood_mode:
                    self.flood(int(command[1]), str(command[-1]))

            else:
                print("Command Invalid: %s" % command)
    
    def flood(self, target, msg):
        if str(self.id) == target:
            return
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            ss.connect((localhost, 50000 + self.succ))
        except ConnectionRefusedError:
            self.search_broadcast()
            ss.connect((localhost, 50000 + self.succ)) 
        content = "[flood] " + str(target) + " " + msg
        print("Send \"" + content + "\" to " + str((localhost, 50000 + self.succ)))
        ss.send(content.encode())
        ss.close()
        time.sleep(4)

    def direct_message(self, target, msg):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect((localhost, 50000 + target))
        content = "[message] " + str(self.id) + " " + msg
        print("Send \"" + content + "\" to " +
              str((localhost, target + 50000)))
        ss.send(content.encode())
        ss.close()

    def search_listener(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ss.bind((localhost, self.id + 50000))
        while True:
            msg, addr = ss.recvfrom(1024)
            msg = msg.decode()
            print("[U] Received \"" + msg + "\" from " + str(addr))
            reply = "[Response] " + str(self.id)
            ss.sendto(reply.encode(), addr)


    # def direct_message(self, target):

    def tcp_listener(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.bind((localhost, self.id + 50000))
        ss.listen(1)

        while True:
            conn, addr = ss.accept()
            thread = threading.Thread(target=self.tcp_handler, args=(conn, addr)) 
            thread.start()

    def tcp_handler(self, ssocket, senderaddr):
        message = ssocket.recv(1024)
        if not message:
            return
        message_decode = message.decode()
        message_split = message_decode.split()
        if message_split[0] == "[message]":
            print("Received from " + message_split[1] +": " + message_split[2])
        elif message_split[0] == "[flood]":
            if message_split[1] != str(self.id):
                print("Flood to " + message_split[1] +": " + message_split[2])
                self.flood(message_split[1], message_split[-1])
            else:
                print("Received message: " + message_split[2])
                #print("flooding...")
   
    def search_broadcast(self):
        alive = []
        for i in port_range:
            if(i == (self.id + 50000)):
                continue
            ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            content = "[search] " + str(self.id)
            print("Send \"" + content + "\" to " +
                  str((localhost, i - 50000)), end=": ")
            ss.sendto(content.encode(), (localhost, i))
            try:
                msg, addr = ss.recvfrom(1024)
                msg = msg.decode().split()
                if(msg[0] == "[Response]"):
                    print("Got response from " + str(addr))
                    alive.append(int(msg[1]))
            except ConnectionResetError:
                print("No reponse")
                continue
            except KeyboardInterrupt:
                print("Ctrl-C")
                return
            ss.close()
        temp_len = len(alive)
        if(temp_len == 0):
            self.pred = self.addr
            self.succ = self.addr
        elif(temp_len == 1):
            self.pred = alive[0]
            self.succ = alive[0]
        else:
            self.pred = -1
            self.succ = -1
            pred_found = False
            succ_found = False
            for i in range(temp_len):
                if not succ_found and alive[i] > self.id:
                    self.succ = alive[i]
                    succ_found = True
                if not pred_found and alive[i-1] < self.id and alive[i] > self.id:
                    self.pred = alive[i - 1]
                    pred_found = True
            if not succ_found:
                self.succ = alive[0]
            if not pred_found:
                self.pred = alive[-1]
        print("My predecessor node: {}".format(self.pred))
        print("My successor node  : {}".format(self.succ))


if __name__ == "__main__":
    if(len(sys.argv) < 2):
        print("Usage: python node.py <id>")
    else:
        node = Node(sys.argv[1])

# to run:
#   python node.py 1
