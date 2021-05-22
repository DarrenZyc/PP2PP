import threading
import sys
import socket
import time
import os
import pathlib
import shutil


localhost = "127.0.0.1"
broadcast_print = False

SEARCH_REFRESH_RATE = 3
DEBUG_MODE = not True

class Node:
    def __init__(self, id):
        self.id = int(id)
        self.port = 50000 + self.id
        self.addr = (localhost, self.id + 50000)
        self.threads = list()
        self.succ = -1
        self.pred = -1
        self.gui_running = False
        self.alive_nodes_ct = -1
        self.alive_nodes = -1
        self.folder_name = "Node{}".format(self.id)
        dirname = pathlib.Path().absolute()
        self.full_fp = os.path.join(dirname, self.folder_name)
        print(self.full_fp)
        if os.path.exists(self.full_fp):
            # os.remove(full_fp)
            shutil.rmtree(self.full_fp)
        os.mkdir(self.full_fp)
        
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

        t4 = threading.Thread(target=self.regular_actions)
        t4.daemon = True
        self.threads.append(t4)
        t4.start()
    
    def regular_actions(self):
        while True:
            self.search_broadcast()
            self.relocate_file()
            time.sleep(SEARCH_REFRESH_RATE)

    def relocate_file(self):
        files = os.listdir(self.full_fp)
        for file in files:
            filename_hash = self.hash(file)
            target_port = filename_hash % (self.alive_nodes_ct + 1) + 1
            if target_port == self.id or self.alive_nodes == -1 or (target_port not in self.alive_nodes):
                continue
            else:
                print(f"relocate {file}")
                filepath = os.path.join(self.folder_name, file)
                self.file_ops(filepath, 1)
                os.remove(filepath)
        
    def console(self):
        while True:
            commandf = input("> ")
            if commandf == "":
                continue
            command = commandf.split()
            if command[0] == "quit" or command[0] == "exit":
                print("Quitting ...")
                sys.exit(0)
            elif command[0] == "usage":
                print("\t\nquit / exit\nsave <file_path>\ndelete/remove <file_path>\nget/peek <filename>\nmessage <target_id> <-d/-f> <content>")
            elif command[0] == "save":
                self.file_ops(command[1], 1)
            elif command[0] == "delete" or command[0] == "remove":
                self.file_ops(command[1], 2)
            elif command[0] == 'get' or command[0] == 'peek':
                self.file_ops(command[1], 3)
            elif command[0] == 'gui':
                t6 = threading.Thread(target=self.gui)
                self.threads.append(t6)
                t6.start()
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
                    self.direct_message(int(command[1]), commandf[13:])
                if flood_mode:
                    self.flood(int(command[1]), commandf[13:])

            else:
                print("Command Invalid: %s" % command)
    

    def file_ops(self, path, mode_id):
        # 1 - save ; 2 - delete; 3 - peek
        if mode_id == 1:
            try:    
                with open(f"{path}", "r") as f:
                    text = f.read()
            except FileNotFoundError:
                print("File Not Found: %s" % path)
                return
            mode = '[save]'
        elif mode_id == 2:
            text = 'NULL'
            mode = '[delete]'
        elif mode_id == 3:
            text = 'NULL'
            mode = '[get]'
        # print(text)
        filename = path.split("\\")[-1]
        send_data = f"{mode} {filename} {text}"
        filename_hash = self.hash(filename)
        target_port = filename_hash % (self.alive_nodes_ct + 1) + 1
        if mode_id == 3 and target_port == self.id:
            try:
                filepath = os.path.join(self.folder_name, filename)
                with open(f"{filepath}", "r") as f:
                    text = f.read()
            except FileNotFoundError:
                print("File Not Found: %s" % path)
                return
            print(text)
            return
        # print(f"[DEBUG] {self.alive_nodes} {target_port}")
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while (target_port + 50000 >= port_range[0] and target_port + 50000 <= port_range[-1]):
            try:
                client.connect((localhost, 50000 + target_port))
                break
            except ConnectionRefusedError:
                target_port = target_port + 1
            # except OSError:
                #print(f"Unsuccessful: {target_port}")
                #return
        client.send(send_data.encode())
        if mode_id == 3:
            data = client.recv(1024)
            print(data.decode())
        client.close()

    def hash(self, string):
        out = 0
        for i in string:
            if i.isdigit():
                out = out + i
            else:
                out = out + (ord(i) - ord('a'))
            out = out * 100
        return out

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
        print("[fwd] \"" + msg + "\" to successor")
        ss.send(content.encode())
        ss.close()
        time.sleep(4)

    def direct_message(self, target, msg):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect((localhost, 50000 + target))
        content = "[message] " + str(self.id) + " " + msg
        #print("Send \"" + content + "\" to " +
        #      str((localhost, target + 50000)))
        ss.send(content.encode())
        ss.close()

    def search_listener(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ss.bind((localhost, self.id + 50000))
        while True:
            msg, addr = ss.recvfrom(1024)
            msg = msg.decode()
            # print("[U] Received \"" + msg + "\" from " + str(addr))
            reply = "[Response] " + str(self.id)
            ss.sendto(reply.encode(), addr)


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
            print("Received from " + message_split[1] +": " + message_decode[12:])
        elif message_split[0] == "[flood]":
            if message_split[1] != str(self.id):
                print("Flood to " + message_split[1] +": " + message_decode[10:])
                self.flood(message_split[1], message_decode[10:])
            else:
                print("Received message: " + message_decode[10:])
                #print("flooding...")
        elif message_split[0] == "[save]":
            name, text = message_split[1], " ".join(message_split[2:])
            filepath = os.path.join(self.full_fp, name)
            with open(filepath, "w") as f:
                f.write(text)
            send_data = "OK@File uploaded successfully."
            ssocket.send(send_data.encode())
        elif message_split[0] == "[get]":
            name = message_split[1]
            try:
                filepath = os.path.join(self.full_fp, name)
                with open(filepath, "r") as f:
                    text = f.read()
            except FileNotFoundError:
                text = f"File Not Found: {name}"
            ssocket.send(text.encode())
        elif message_split[0] == "[delete]":
            name, text = message_split[1], message_split[2]
            filepath = os.path.join(self.full_fp, name)
            files = os.listdir(self.full_fp)
            if name in files:
                os.remove(filepath)
                print("Remove successful")
        else:
            print(message_decode)
        ssocket.close()
   
    def search_broadcast(self):
        alive = []
        for i in port_range:
            if(i == (self.id + 50000)):
                continue
            ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            content = "[search] " + str(self.id)
            if broadcast_print:
                print("Send \"" + content + "\" to " + str((localhost, i - 50000)), end=": ")
            ss.sendto(content.encode(), (localhost, i))
            try:
                msg, addr = ss.recvfrom(1024)
                msg = msg.decode().split()
                if msg[0] == "[Response]":
                    if broadcast_print:
                        print("Got response from " + str(addr))
                    alive.append(int(msg[1]))
            except ConnectionResetError:
                if broadcast_print:
                    print("No reponse")
                continue
            
            ss.close()
        temp_len = len(alive)
        self.alive_nodes = alive
        self.alive_nodes_ct = temp_len
        if(temp_len == 0):
            self.pred = self.id
            self.succ = self.id
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
        if DEBUG_MODE:
            if temp_len == 0:
                print(f"{self.id} ⟲")
            elif temp_len == 1:
                print(f"{self.id} ⇆ {self.succ}")
            else:
                print(f"{self.pred} -> {self.id} -> {self.succ}")
                
port_range = range(50001, 50010)
if __name__ == "__main__":
    
    if(len(sys.argv) < 2):
        print("Usage: python node.py <id>")
    else:
        node = Node(sys.argv[1])

# to run:
#   python node.py 1
