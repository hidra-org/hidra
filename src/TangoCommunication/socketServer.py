#!/usr/bin/env python
#
import thread, os, socket

PORT = 7651

#
# assume that the server listening to 7651 serves p09
#
port2BL = {
    "7651": "P09",
    "7652": "P09",
    "7653": "P09",
    "7654": "P09"}

class Eiger():
    '''
    this class holds getter/setter for all parameters
    and function members that control the operation.
    '''
    def __init__( self, Beamline):
        #
        # Beamline is read-only, determined by portNo
        #
        self.Beamline = Beamline
        #
        # the Eiger address and port number
        #
        self.host = None
        self.port = None
        #
        # the file names will be:
        #   /gpfs/current/raw/ge_00005/eigerName/ge_000005_<EigerGeneratedNo>.cbf
        #
        #  FileDir: /gpfs/current/raw/ge_00005/eigerName
        #
        self.FileDir = "None"
        #
        #  FilePrefix: ge_00005
        #
        self.FilePrefix = "None"
        #
        #  FilePostfix: .cbf
        #
        self.FilePostfix = "None"
        return

    def execMsg( self, msg):
        '''
        set filedir /gpfs/current/raw/ge_00005
          returns DONE
        get filedir
          returns /gpfs/current/raw/ge_00005
        do reset
          return DONE
        '''
        tokens = msg.split( ' ')
        if len( tokens) == 0:
            return "ERROR"

        if tokens[0].lower() == 'set':
            if len( tokens) != 3:
                return "ERROR"
            return self.set( tokens[1], tokens[2])
        elif tokens[0].lower() == 'get':
            if len( tokens) != 2:
                return "ERROR"
            return self.get( tokens[1])
        elif tokens[0].lower() == 'do':
            if len( tokens) != 2:
                return "ERROR"
            return self.do( tokens[1])
        else:
            return "ERROR"

    def set( self, param, value):
        '''
        set a parameter, e.g.: set filedir /gpfs/current/raw/ge_00005
        '''
        if param.lower() == "filedir":
            self.FileDir = value
            return "DONE"
        elif param.lower() == "fileprefix":
            self.FilePrefix = value
            return "DONE"
        elif param.lower() == "filepostfix":
            self.FilePostfix = value
            return "DONE"
        else:
            return "ERROR"

    def get( self, param):
        '''
        return the value of a parameter, e.g.: get filedir
        '''
        if param.lower() == "filedir":
            return self.FileDir
        elif param.lower() == "fileprefix":
            return self.FilePrefix
        elif param.lower() == "filepostfix":
            return self.FilePostfix
        else:
            return "ERROR"

    def do( self, cmd):
        '''
        executes commands
        '''
        if cmd.lower() == "reset":
            return self.reset()
        elif cmd.lower() == "start":
            return self.start()
        else:
            return "ERROR"

    def reset( self):
        return "DONE"

    def start( self):
        '''
        start ...
        '''
        #
        # see, if all required params are there.
        #
        if self.FileDir == 'None':
            return "ERROR"
        if self.FilePrefix == 'None':
            return "ERROR"
        if self.FilePostfix == 'None':
            return "ERROR"
        #
        # execute the start action ...
        #
        return "DONE"

class sockel( object):
    '''
    '''
    #
    # one socket for the port, accept() generates new sockets
    #
    sckt = None

    def __init__( self):
        if sockel.sckt is None:
            print "create the main socket"
            try:
                sockel.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            except Exception, e:
                print "socket() failed", e
                sys.exit()
            self.host = socket.gethostname()
            try:
                sockel.sckt.bind( (self.host, PORT))
            except Exception, e:
                raise Exception( "sockel.__init__: bind() failed %s" % str(e))

            self.port = PORT
            print "bind( %s, %d) ok" % (self.host, self.port)
            sockel.sckt.listen(5)
        self.conn, self.addr = sockel.sckt.accept()

        if not str(PORT) in port2BL.keys():
            raise Exception( "sockel.__init__: port %d not identified" % str(PORT))

        self.eiger = Eiger( port2BL[ str(PORT)])

    def close( self):
        #
        # close the 'accepted' socket only, not the main socket
        # because it may still be in use by another client
        #
        if not self.conn is None:
            self.conn.close()

    def finish( self):
        sockel.sckt.close()

    def recv( self):
        argout = None
        try:
            argout = self.conn.recv(1024)
        except Exception, e:
            print e
            argout = None
        print "recv (len %2d): %s" % (len( argout.strip()), argout.strip())
        return argout.strip()

    def send( self, msg):
        try:
            argout = self.conn.send( msg)
        except:
            argout = ""
        print "sent (len %2d): %s" % (argout, msg)
        return argout

def socketAcceptor():
    # waits for new accepts on the original socket,
    # receives the newly created socket and
    # creates threads to handle each client separatly
    while True:
        s = sockel()
        print "socketAcceptor: new connect"
        thread.start_new_thread( socketServer, (s, ))

def socketServer(s):
    global msgBuf
    while True:
        msg = s.recv()
        if len( msg) == 0:
            print "received empty msg"
            continue
        if msg.lower().find('bye') == 0:
            print "received 'bye', closing socket"
            s.close()
            break
        if msg.find('exit') >= 0:
            s.close()
            s.finish()
            os._exit(1)
        reply = s.eiger.execMsg( msg)
        if s.send( reply) == 0:
            s.close()
            break


if __name__ == '__main__':
    socketAcceptor()

