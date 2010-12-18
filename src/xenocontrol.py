#!/usr/bin/python -u
# -u == unbuffered for automation ! /usr/bin/env python -u don't work so I don't use it.

## Copyright (c) 2008-2010 ENIX S.A.R.L
##
## This file is part of XenoControl.
##
## XenoControl is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## XenoControl is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with XenoControl. If not, see <http://www.gnu.org/licenses/>.

import os, sys, select, re, time, fcntl, signal, random
from pprint import pprint,pformat
from socket import gethostname, gethostbyname
from fnmatch import fnmatch
from glob import glob
hostname = gethostname()
ip = gethostbyname(hostname)
from datetime import datetime
from optparse import OptionParser
from asyncore import compact_traceback
from Queue import Queue, Empty
import threading
import textwrap # to wrap help text
try:
    import spread
except:
    print "You must have python-spread installed"
    sys.exit(1)


VERSION="0.444"
EXECUTE_ENABLED=True




################################################################################
#
# Libs function
#
################################################################################


def AutoDict(mydict, additionnal = None, condition=None):
    """
    Factory of metaclass used to fill a dictionary with the class name and
    the class object. The dictionary can the be used in a factory to instanciate
    the appropriate class for the context.

    mydict parameters is the dict to be filled with
    
    additionnal could be used to map your class object to the value of an attribute
    of your class.

    condition is a function that take the class in parameters and return true
    if the class have to be in the dict. By default, everything is.
    """
    class Meta(type):
        def __init__(cls, name, bases, dict):
            super(Meta, cls).__init__(name, bases, dict)
            if condition and condition(cls) or not condition:
                mydict[name.lower()] = cls
                if additionnal:
                    mydict[dict[additionnal]] = cls
    return Meta


def debug(string):
    print string

def atot(*l, **kv):
    return (l, kv)

def seval(data):
    """
    Pseudo-secure (ie, not extensivly checked) eval
    """
    return eval(data.strip(),{"__builtins__":None},{"True": True, "False": False})

def deltatime(datetimeobj):
    """
    Convert time from DateTime class in xmlrpc and give delta
    """
    return (datetime.now() - datetime.strptime(datetimeobj.value, "%Y%m%dT%H:%M:%S")).seconds

def megabyze(i):
    """
    Return the size in Kilo, Mega, Giga, Tera, Peta according to the input.
    """
    i = float(i)
    for unit in "", "K", "M", "G", "T", "P":
        if i < 2000: break 
        i = i / 1024
    return "%.1f%s"%(i, unit)

cmdqueue = Queue()
locks = {}

def lock(lockname):
    """
    Return a locking decorator using the lock lockname :). 
    Sample :
    @lock("something")
    def toto(): pass
    """
    global locks
    lockname = lockname.lower() # Case safe
    # this will be used as a decorator, so we don't need it to be thread safe
    mylock = locks.get(lockname, threading.Lock())
    locks[lockname] = mylock
    def decorator(func):
        def f(*l, **kv):
            if not mylock.acquire(): raise Exception, "Cannot acquire lock !"
            try:
                result = func(*l, **kv)
            finally:
                mylock.release()
            return result
        return f
    return decorator

from subprocess import Popen, PIPE, STDOUT
def execute(cmd, raiseonfailure=False, timeout=0, notify=None, tosend=""):
    """
    Execute cmd by asking the mainthread to do the exec/fork.
    Return the stdout buffer of the cmd. Raise if raiseonfailure is true
    and the process' return code is not 0.
    If timeout is set (in seconds), the cmd is killed and an exception is
    trown if it didn't terminate within the allowed timing.
    Notify can be a function that'll be called with every chunk readed from cmdline.
    """
    print "Executing : %s"%repr(cmd)
    if __name__ != "__main__":
        # Ugly hack, have to be redesigned when we split all this mess over different file/libs
        print "Warning, executing the command in the current thread as we're used as a lib."
        reply = Popen(cmd, shell=True, 
                      stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    else:
        # Create a channel to send the reply
        reply_channel = Queue(1)
        # On envoit la commande ...
        cmdqueue.put((cmd, reply_channel))
        reply = reply_channel.get()
    if not reply:
        return "No command specified"
    if not timeout:
        result = reply.communicate()[0]
    else:
        try:
            tt = t = time.time()
            result = []

            # Set to non-blocking
            flags = fcntl.fcntl(reply.stdout, fcntl.F_GETFL)
            fcntl.fcntl(reply.stdout, fcntl.F_SETFL, flags| os.O_NONBLOCK)
            if tosend:
                print "tosend : %s"%repr(tosend)
                reply.stdin.write(tosend)
                reply.stdin.close()
            # Just loop !
            while not reply.stdout.closed and tt - t < timeout:
                if select.select([reply.stdout], [], [], max(timeout - (tt - t),0))[0]:
                    tmp = reply.stdout.read()
                    result.append(tmp)
                    if notify: notify(tmp)
                    if not tmp:
                        reply.wait()
                        break
                tt = time.time()
            print "Executed in %.2f"%(tt -t)

            # Let's collect what we could have missed during our loop and build the result.
            try: result.append(reply.stdout.read())
            except: pass
            result = "".join(result)

            # Cleanup
            reply.stdout.close()
            if reply.poll() == None:
                try: os.kill(reply.pid, signal.SIGKILL)
                except OSError: pass
                raise Exception,"Timeout reached '%s' : %s (%s)"%(cmd, reply.returncode, result)
        except Exception, e:
            print e
            raise e
    if raiseonfailure and reply.returncode:
        debug("   Bad return code for %s : %s (%s)"%(cmd, reply.returncode, result))
        raise Exception,"Bad return code for %s : %s (%s)"%(cmd, reply.returncode, result)
    return result


class StatusTracker(object):
    """
    Class used to keep track of what's going on in the cluster.
    
    FIXME: in dev, must be completed
    """
    def __init__(self, name="", initialdata=None, maxsize=1000):
        self.name = name
        self.data = []
        self.childs = []
        self.maxsize = maxsize
        if initialdata:
            if isinstance(initialdata, list):
                for a in initialdata:
                    self.append(a)
            else:
                self.append(initialdata)
                
    def append(self, data):
        if isinstance(data, tuple):
            self.data.append(data)
        else:
            if len(self.data) >= self.maxsize: self.data.pop(0)
            self.data.append((time.time(), data))
    set = append
    
    def sub(self, name, initialdata=None):
        tmp = StatusTracker(name, initialdata)
        self.childs.append(tmp)
        return tmp
    
    def serialize(self):
        return (self.name, self.data, [ i.serialize() for i in self.childs ])
    
    def __repr__(self):
        return repr(self.serialize())

    def __str__(self):
        tmp = self.get()
        return "[%s] %s"%(tmp)
    
    @classmethod
    def deserialize(cls, data):
        name, data, childs = data
        result = cls(name=name, initialdata=data)
        for child in childs:
            result.childs.append(cls.deserialize(child))
        return result
    
    @classmethod
    def fromstring(cls, data):
        data = seval(data)
        return cls.deserialize(data)
    
    def get(self):
        items = [ item for item in [ child.get() for child in self.childs ] if item ]
        if self.data:
            items.append(self.data[-1])
        if not items: return None
        item = max(items)
        return (item[0], "%s : %s"%(self.name, item[1]))
    
    def getlog(self, formatted=True):
        result = [ (i[0], "%s : %s"%(self.name, i[1])) for i in self.data ]
        for child in self.childs:
            result += [ (i[0], "%s : %s"%(self.name, i[1])) for i in child.getlog(formatted=False) ]
        if formatted:
            result.sort()
            return [ "[%s] %s"%(t,s) for t, s in result ]
        return result

    
    

#-------------------------------------------------------------------------------
#
# END of libs
#
#-------------------------------------------------------------------------------


################################################################################
#
# Communication factories
#
################################################################################

#MSG TYPE:
CONTENT_TYPE = { 'REQUEST': 1,
                 'REPLY': 2,
                 'ERROR': 3,
                 'AUTOMATION_ACK': 4 }
globals().update(CONTENT_TYPE)
RCONTENT_TYPE = dict([ (v,k) for k, v in CONTENT_TYPE.items() ])

SPGRP="all"

CODE_OK = "Ok"
CODE_KO = "KO"
SPREADLEVEL=spread.SAFE_MESS


class Request(object):
    """
    Class used to easily manipulate request and transmit them on the network
    """
    MSG_TYPE=REQUEST
    def __init__(self, query, sequence=None, selector=None, sender=None, target=SPGRP):
        if not sequence: sequence = int(time.time()*1000000)
        self.sequence = sequence
        if not selector: selector = {}
        self.selector = selector
        self.query = query
        if not isinstance(target, list):
            target = [ target ]
        self.target = target
        self.sender = sender
        self.sended = set([])
        
    @classmethod
    def from_network(cls, spreadmsg):
        """
        Factory to generate the appropriate Request object with the data received
        from the Network. (generated by the to_network method).
        """
        # ['endian', 'groups', 'message', 'msg_type', 'sender']
        sequence, selector, query, sender = seval(spreadmsg.message)
        return cls(query=query, sequence=sequence, selector = selector, sender=sender, target = list(spreadmsg.groups))

    def to_network(self):
        """
        Serialize the current instance and send it on the network. It return a list
        of string that can be converted back to Request object with the classmethod
        from_network(data).

        Typical usage in spread setup :
        for msg in request.to_network():
            spread.multicast(*msg)
        return format : (SPREADLEVEL, target, content, content_type)        
        """
        result = []
        for target in self.target:
            result.append((SPREADLEVEL, target, "%s"%self, self.MSG_TYPE))
        return result

    
    def __str__(self):
        return repr((self.sequence, self.selector, self.query, self.sender))
    
    def __repr__(self):
        return "%s"%self
    
    def respond(self, message=None):
        """
        Generate an Response object for the request by setting the appropriate
        sequence number and the appropriate target for the message.
        """
        return Reply(message = message, sequence = self.sequence, target = self.sender)
    
    def ack(self, group):
        """
        Send a Ack response with appropriate sequence number.

        Used in automation to keep track of who received what in automation task so we
        can nicely take over if the master fail.
        """
        return AutoAck(message = "Ack", sequence = self.sequence, target = group)
    
    def rerror(self, message=None):
        """
        Send the specified message as an Error response message, copying the sequence
        number and setting the target from the instancied Request object.
        """
        return Error(message = message, sequence = self.sequence, target = self.sender)
        
    def set_sended(self, sended):
        """
        Used to keep track of the list of host to which the request has been sended.
        Only available on the sending side.
        """
        self.sended = sended


Replys = {}
class Reply(object):
    """
    Class used to manipulate reply and transmit it on the network.

    A reply is, in fact, always a list of reply. All the reply of a
    request are agregated in one big reply returned to the request sender.
    No host tracking is done when this aggregation occur, so if you want
    to know who sended what, you have to include this information in your
    response.
    It'll allow us to proxyfy the request/reply object in the futur, so
    we don't rely on one big broadcast network.

    You can concatenate 2 reply object by using the + or the += operator.

    You can also iterate on the reply object to get all the individual
    response.
    """
    __metaclass__ = AutoDict(Replys, "MSG_TYPE")
    MSG_TYPE = REPLY
    def __init__(self, message=None, sequence=0, target=SPGRP, sender=None):
        self.sequence = sequence
        if message == None: message = []
        if not isinstance(message,list):
            if message is None: message = []
            else: message = [ message ]
        self.message=message
        self.target = target
        self.sender = sender
        self.sended = set([])
        
    @classmethod
    def from_network(cls, spreadmsg):
        """
        Factory to generate the appropriate Reply object with the data received
        from the Network. (generated by the to_network method).
        """
        # ['endian', 'groups', 'message', 'msg_type', 'sender']
        realcls = Replys[spreadmsg.msg_type]
        sequence, message, sender = seval(spreadmsg.message)
        return realcls(message=message, sequence=sequence, sender=sender, target=list(spreadmsg.groups))
    
    def to_network(self):
        """
        Serialize the object instance and send it on the network. It return a list
        of string that can be converted back to Reply object with the classmethod
        from_network(data).

        Typical usage in spread setup :
        for msg in reply.to_network():
            spread.multicast(*msg)
        return format : (SPREADLEVEL, target, content, content_type)        
        """
        return [ (SPREADLEVEL, self.target, "%s"%self, self.MSG_TYPE) ]
    
    def __str__(self):
        return repr((self.sequence, self.message, self.sender))
    
    def set_sended(self, sended):
        """
        Used to keep track of the list of host to which the request has been sended.
        Only available on the sending side.
        """
        self.sended = sended
        
    def __add__(self, other):
        """
        Override the + operator so we can easily concatenate 2 reply object.
        """
        if self.sequence != other.sequence:
            raise ValueError, "Error, sequence missmatch (%s <> %s)"%(self.sequence, other.sequence)
        if self.target != other.target:
            raise ValueError, "Error, target missmatch (%s <> %s)"%(self.target, other.target)
        return Reply(message = self.message + other.message, sequence = self.sequence, target=self.target)
    
    def __iadd__(self, other):
        """
        Override the += operator so we can easily concatenate 2 reply object.
        """
        if self.sequence != other.sequence:
            raise ValueError, "Error, sequence missmatch (%s <> %s)"%(self.sequence, other.sequence)
        if self.target != other.target:
            raise ValueError, "Error, target missmatch (%s <> %s)"%(self.target, other.target)
        self.message += other.message
        return self
    
    def __iter__(self):
        for a in self.message:
            yield a
            
    def __len__(self):
        return len(self.message)


class Error(Reply):
    """
    Class derived from the Reply object used to return an error.
    """
    MSG_TYPE = ERROR

class AutoAck(Reply):
    """
    Class derived from the Reply object used to return an automation ack.
    """
    MSG_TYPE=AUTOMATION_ACK


# Helper pour la construction du message a passer comme Reponse.
def buildresult(result, identifier):
    if not result: return None
    if not isinstance(result, dict):
        result = { "identifier": identifier, "result": result }
    else:
        result["identifier"] = result.get("identifier", identifier)
    return result


#-------------------------------------------------------------------------------
#
# END of Communication factories
#
#-------------------------------------------------------------------------------


################################################################################
#
# Server's stuff
#
################################################################################


class VM(object):
    """
    Class used to manipulate a vm instance (running or stopped)

    There is no defined API yet, as we handle only xen virtualisation for now,
    but lot's of method of this class should be taken as is when time come to
    support other virtualisation software.
    """
    def __init__(self, host, uuid=None, name=None):
        self.host = host
        self.api = host.api
        if uuid and not name:
            name = self.get_name_from_uuid(uuid)            
        if not name: raise "Error, we cannot find our name (did you specified it ?)"
        self.name = name
        self.config = self.read_config()
        
    def __repr__(self):
        #return repr(self.info())
        return "VM class %s"%self.name
    
    def __str__(self):
        return self.name
    
    def get_name_from_uuid(self, uuid):
        return self.api.VM.get_name_label(uuid)
    
    def uuid(self):
        try: return self.api.VM.get_by_name_label(self.name)[0]
        except: return "No-UUID-Not-Running"
        
    def get_power_state(self):
        """
        Return the power state of the VM
        """
        try: return self.api.VM.get_power_state(self.uuid())
        except: return "Halted"
        
    def running(self):
        """
        Return True if the vm is running
        """
        return self.get_power_state() == 'Running'
    
    def paused(self):
        """
        Return True if the vm is paused
        """
        return self.get_power_state() == 'Paused'
    
    def halted(self):
        """
        Return True if the vm is halted.
        """
        return self.get_power_state() == 'Halted'
    
    def start(self):
        """
        Start the VM
        """
        if not self.running():
            result = execute("xm create /etc/xen/auto/%s"%self.name, timeout=30)
            return result
        return "VM is already running"
    
    def stop(self, forced=False):
        """
        Stop the VM. If forced == True, use a hard shutdown.
        """
        if not self.running(): return "VM is not running"
        if forced:
            self.api.VM.hard_shutdown(self.uuid())
            try:
                time.sleep(1)
                result = execute("xm destroy %s"%self.name)
                # Sometime this API call don't work ...
                #self.api.VM.destroy(self.uuid())
            except: pass
        else:
            self.api.VM.clean_shutdown(self.uuid())
        return CODE_OK
    
    @lock("restartvm")
    def restart(self, forced=False):
        """
        Restart the vm by manually stopping/starting it.
        If forced == True, stop it with a hard shutdown.
        """
        self.stop(forced)
        tt = time.time()
        while not self.halted() and time.time() - tt <= 60:
            # We wait the vm to stop.
            time.sleep(1)
        if self.halted():
            self.start()
            return CODE_OK
        return CODE_KO
    
    @lock("migration")
    def migrate(self, remote):
        """
        Ask for live migration of the vm to 'remote' host.
        """
        if self.running():
            result = execute("xm migrate --live %s %s"%(self.name, remote), timeout=600)
	time.sleep(1)
        return CODE_OK
    
    def pause(self):
        """
        Pause the vm.
        """
        if self.running():
            self.api.VM.pause(self.uuid())
            return CODE_OK
        return "Not running"
    
    def unpause(self):
        """
        UnPause a paused vm.
        """
        try: # On force vu qu'on a un cache sur le self.paused
            self.api.VM.unpause(self.uuid())
            return CODE_OK
        except:
            return "Not paused"
    
    def set_memory(self, memory):
        """
        Set dynamicaly the memory of the running instance.
        FIXME : Add a flag to save it in config file.
        """
        try:
            # Ca c'est la methode propre mais ca marche pas si on met plus de 2048 Mo
            # (integer sur l'appel rpc) ... supair.
            #self.api.VM.set_memory_dynamic_max(megabytes*1024*1024)
            #self.api.VM.set_memory_dynamic_min(megabytes*1024*1024)
            execute("xm mem-set %s %s"%(self.name, memory), timeout=5)
            return CODE_OK
        except:
            return CODE_KO
        
    def set_cpu(self, cpu):
        """
        Set dynamicaly the cpu count of the running instance.
        FIXME : config file.
        """
        try:
            execute("xm vcpu-set %s %s"%(self.name, cpu), timeout=5)
            return CODE_OK
        except:
            return CODE_KO
        
    def confpath(self):
        """
        Return the full path of the config file.
        """
        result = [ os.path.join(d, self.name) for d in self.host.CONFDIR if os.path.isfile(os.path.join(d, self.name)) ]
        if result: return result[0]
        else: return ""
        
    def read_config(self):
        """
        Read the config file, interpret it and return a python dictionnary
        """
        globs = {}
        locs = {}
        confpath = self.confpath()
        if not confpath: return {}
        try:
            execfile(confpath, globs, locs)
        except: # if we have a problem while reading the file, return nothing.
            return {}
        locs["realdisks"] = {}
        for device, name, mode in [ d.split(",") for d in locs["disk"] ]:
            if not device.startswith("phy:"): continue # We don't handle file based device & co
            device = device.replace("phy:", "/dev/").replace("/dev//dev/", "/dev/")
            if ":" in name: name = name.split(":", 1)[1]
            locs["realdisks"][name] = device
        locs["plaintext"] = file(confpath).read()
        locs["confpath"] = confpath
        return locs
    
    def remove_config(self):
        """
        Delete the vm config file.
        """
        os.unlink(self.confpath())
        
    def info(self):
        """
        Return partial (quickly gathered) informations on the vm.
        """
        return self.stats(False)
    
    def stats(self, all=True):
        """
        Return full informations about the vm (including stats).
        """
        try:
            tmp = self.host.metrics("VM", self.uuid(), all)[0]
        except:
            tmp = {}
        if self.config:
            if "vbds" not in tmp:
                tmp["vbds"] =  [ { "name": i } for i in self.config["realdisks"].keys() ]
            for vbd in tmp.get("vbds", []):
                if not vbd["name"] in self.config["realdisks"]: continue
                path = self.config["realdisks"][vbd["name"]]
                vbd["realpath"] = path
                if all:
                    path = path.split("/")
                    vbd.update(self.host.vg[path[-2]].lv_info(path[-1]))
        tmp["config"] = self.config
        tmp["host"] = self.host.name
        tmp["power_state"] = self.get_power_state()
        tmp[tmp["power_state"]] = 1
        if "name" not in tmp: tmp["name"] = self.name
        if "vcpu" not in tmp: tmp["vcpu"] = self.config.get("vcpus", 0)
        if "memMB" not in tmp: tmp["memMB"] = long(self.config.get("memory", 0))
        for label in ( "vcpu", "memMB" ):
            tmp[label] = long(tmp[label])
        return tmp
    
    def delete(self):
        """
        Delete the vm, including the disk and the config file,
        stopping it if necessary.
        """
        # Stop the VM
        self.stop(forced=True)
        i = 0
        while not self.halted() and i < 50:
            time.sleep(0.2)
        # Destroy the corresponding devices
        for vbdpath in self.config["realdisks"].values():
            vbdpath = vbdpath.split("/")
            tmp = self.host.vg[vbdpath[-2]].lv_remove(vbdpath[-1])
        # Removing the config file
        self.remove_config()
        return CODE_OK
    
    @lock('drbd')
    def drbd_create_target(self, lvlist):
        """
        Create DRBD target on the receiving side during a migration
        """
        result = {}
        for path, size in lvlist:
            print path, size
            name = os.path.basename(path)
            self.host.vg.values()[0].lv_create(name, size)
            result[path] = self.host.drbd.create(path)
            self.host.drbd.secondary(path)
        return result
    
    @lock('drbd')
    def drbd_swap_to(self):
        """
        Swap all the disk of the vm on drbd target transparently
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        result = {}
        for path in paths:
            result[path] = self.host.drbd.create(path)
            self.host.drbd.primary(path)
        pause = self.running()
        try:
            if pause: self.pause()
            for path in paths:
                self.host.drbd.switch_to_drbd(path)
        finally:
            if pause: self.unpause()
        return result
    
    @lock('drbd')
    def drbd_connect(self, remote, portmap, rate=50000):
        """
        Connect all the previously created drbd (via swap_to or create_target) to
        the remote side
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        result = {}
        for path in paths:
            result[path] = self.host.drbd.connect(path, remote, portmap[path], rate)
        return result
    
    @lock('drbd')
    def drbd_swap_from(self):
        """
        Switch back the device of the vm on 'raw' disk.
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        pause = self.running()    
        try:
            if pause: self.pause()
            exception = None
            for path in paths:
                try:
                    self.host.drbd.switch_from_drbd(path)
                except Exception, e:
                    exception = e
            if exception: raise exception
        finally:
            if pause: self.unpause()        
            for path in paths:
                self.host.drbd.destroy(path)
        return CODE_OK
    
    def drbd_wait_sync(self):
        """
        Wait for the syncronisation of all drbd device used
        by the VM
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        for path in paths:
            self.host.drbd.wait_sync(path)
        return CODE_OK
    
    @lock('drbd')
    def drbd_primary(self):
        """
        Switch all DRBD device to primary mode.
        DRBD MUST support multi-primary mode if you want this to work.
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        for path in paths:
            self.host.drbd.primary(path)
        return CODE_OK
    
    @lock('drbd')
    def drbd_secondary(self):
        """
        Switch all DRBD device to secondary mode
        """
        paths = [ vbd["realpath"] for vbd in self.info()["vbds"] ]
        for path in paths:
            self.host.drbd.secondary(path)
        return CODE_OK






class XenMetrics(object):
    """
    Class used to get & cache xend metrics.
    Usage :
      metrics = XenMetrics(xenapi)
      metrics("VM", self.uuid(), True)
    """
    def __init__(self, xenapi):
        self.api = xenapi
        self.cache = {}
        self.last_update = {}
        
    @lock("xenmetrics")
    def buildcache(self, datatype):
        """
        Internal function to rebuild the cache by querying the
        xend and storing the result in self.cache.
        It exist only to circumvent xend's slowness
        """
        tt = time.time()
        if tt - self.last_update.get(datatype, 0) > 3:
            tmp = {}
            tmp = getattr(self.api, datatype).get_all_records()
            metrics = getattr(self.api, "%s_metrics"%datatype).get_all_records()
            [ v.__setitem__("metrics", metrics[v["metrics"]]) for v in tmp.values() ]
            self.cache[datatype] = tmp
            self.last_update[datatype] = tt
            
    def VM_extract(self, data, all=True):
        """
        Remap the data to our internal format.
        """
        tmp = {}
        tmp["name"] = data["name_label"]
        tmp["dom0"] = data["is_control_domain"]
        tmp["power_state"] = data["power_state"]
        tmp["uuid"] = data["uuid"]
        tmp["host"] = hostname
        tmp["host_uuid"] = data["resident_on"]
        if all:
            tmp["vbds"] = self("VBD", data["VBDs"])
            tmp["vifs"] = self("VIF", data["VIFs"])
        metrics = data["metrics"]
        tmp["vcpu"] = int(metrics["VCPUs_number"])
        tmp["vcpu_usage"] = metrics["VCPUs_utilisation"]
        tmp["mem"] = int(metrics["memory_actual"])
        tmp["memMB"] = tmp["mem"]/1024/1024
        tmp["uptime"] = deltatime(metrics["start_time"])
        return tmp
    
    def VBD_extract(self, data, all=True):
        """
        Remap the data to our internal format
        """
        tmp = {}
        tmp["name"] = data["device"]
        tmp["mode"] = data["mode"]
        data = data["metrics"]
        tmp["io_read_kbs"] = data["io_read_kbs"]
        tmp["io_write_kbs"] = data["io_write_kbs"]
        tmp["timedelta"] = deltatime(data["last_updated"])
        return tmp
    
    def VIF_extract(self, data, all=True):
        """
        Remap the data to our internal format
        """
        tmp = {}
        tmp["name"] = data["device"]
        tmp["mac"] = data["MAC"]
        data = data["metrics"]
        for a in ( 'io_total_read_kbs', 'io_read_kbs', 'io_total_write_kbs', 'io_write_kbs'):
            tmp[a] = data[a]*1024
        tmp["timedelta"] = deltatime(data["last_updated"])
        return tmp
    
    def __call__(self, datatype, uuidlist=None, all=True):
        """
        Retrieve requested datatype for requested uuidlist.
        """
        self.buildcache(datatype)
        if uuidlist is None:
            uuidlist = self.cache[datatype].keys()
        if type(uuidlist) != list: uuidlist = [ uuidlist ]
        result = []
        for uuid in uuidlist:
            data = self.cache[datatype].get(uuid, None)
            if data:
                tmp = getattr(self, "%s_extract"%datatype, lambda a: a)(data, all)
                result.append(tmp)
        return result


class VGManager(object):
    """
    Class to manage LVM volume group on host
    """
    def __init__(self, vgname):
        self.vgname = vgname
        self.vgpath = os.path.join("/dev", self.vgname)
        
    def lv_path(self, name):
        """
        Return the path for a LV in the VG.
        """
        return os.path.join(self.vgpath, name)
    
    def lv_create(self, name, size):
        """
        Create a new LV in the current VG.
        """
        return execute("/sbin/lvcreate -n '%s' -L '%d'K %s"%(name, size/1024, self.vgpath), True, timeout=20)
    
    def lv_remove(self, name):
        """
        Remove a LV from the current VG.
        """
        lvpath = self.lv_path(name)
        return execute("/sbin/lvremove -f %s"%lvpath, True, timeout=20)
    
    def vg_info(self):
        """
        Retreive information about the VG.
        """
        result = {}
        try:
            tmp = execute("/sbin/vgdisplay --unit b '%s'"%self.vgpath, timeout=5)
            result["storage_size"] = long(re.findall("VG Size +([0-9]+)", tmp)[0])
            result["storage_sizeGB"] = result["storage_size"]/1024/1024/1024
            result["storage_free"] = long(re.findall("Free +PE / Size +[0-9]+ / ([0-9]+)", tmp)[0])
            result["storage_freeGB"] = result["storage_free"]/1024/1024/1024
        except: pass
        return result
    
    def lv_info(self, name):
        """
        Retrieve LV information. LV have to be in the current VG.
        """
        lvpath = self.lv_path(name)
        result = {}
        try:
            tmp=execute("lvdisplay --unit b %s"%lvpath, timeout=5)
            result["size"]= long(re.findall("LV Size +([0-9]+)", tmp)[0])
            # on essaye de chopper l'usage ...
            tmp=execute("dumpe2fs -h %s"%lvpath, timeout=5)
            result["free"] = long(re.findall("Free blocks: +([0-9]+)", tmp)[0]) * long(re.findall("Block size: +([0-9]+)", tmp)[0])
        except:
            pass
        return result
    
    def lv_list(self):
        """
        List the LV in current VG.
        """
        result = []
        tmp = execute("lvs --unit b", timeout=5)
        for name, vg, flags, size in re.findall(" *(?P<name>[^ ]+) +(?P<vg>[^ ]+) +(?P<flags>[^ ]+) (?P<size>[0-9]+)B", tmp):
            t = {}
            t["name"] = name
            t["vg"] = vg
            t["size_bytes"] = long(size)
            t["size"] = megabyze(long(size))
            t["flags"] = flags
            result.append(t)
        return result
    
    def lv_resize(self, name, newsizeGB):
        """
        Resize the LV. Warning : Force resize even if you shrink it to much !
        FIXME : we should test if the lv is in use and resize it with the fs tools if we can.
        """
        return execute("lvresize -f -L %sG %s"%(newsizeGB, self.lv_path(name)), timeout=10)
    
    def lv_remove(self, name):
        """
        Remove the lv from the VG. Data will be loss.
        """
        lvpath = self.lv_path(name)
        tmp = ""
        i = 0
        while "successfully" not in tmp and i < 50:
            tmp = execute("lvremove -f %s"%lvpath, timeout=20)
            i += 1
            time.sleep(0.2)
        return tmp
    
    @classmethod
    def guess_managers(cls):
        """
        Return one VGManager instance for each VG on the host.
        """
        result = {}
        try:
            for name in re.findall("VG Name +(.*)",os.popen("vgdisplay").read()):
                result[name] = cls(name)
        except: pass
        return result


class DRBDManager(object):
    """
    Interface with DRBD

    This class is able to pass any lv (device mapper) device on drbd, create a same size
    device on an other host, connect them, and reswap it from drbd on physical device
    once synced.

    Typical usage:
    both:
      path = '/dev/vg/mydisk'
    node1:
      tmp = DRBDManager(vgmanager)
      sizefornode2 = tmp.
      portfornode2 = tmp.create(path)
      tmp.switch_to_drbd(path)
      tmp.primary(path)
    node2:
      vgmanager.lv_create(name=path, size=sizefornode2)
      tmp = DRBDManager(vgmanager)
      portfornode1 = tmp.create(path)
      tmp.switch_to_drbd(path)
      tmp.secondary(path)
      tmp.connect(node1, portfornode2)
    node1:
      tmp.connect(node2, portfornode1)
    both:
      tmp.wait_sync(path)
    ... once sync is done ...
    node2:
      tmp.primary(path)
    node1:
      tmp.switch_from_drbd(path)
      tmp.destroy(path)
      vgmanager.lv_remove(path)
    node2:
      tmp.switch_from_drbd(path)
      tmp.destroy(path)
    # all our data is now on node2
    
    """
    BASEPORT = 7788
    def __init__(self, vgmanager):
        """
        Init function.
        Take a vgmanager used to create the lv device on the receiving side.
        """
        self.drbds = dict([ ("drbd%d"%i, None) for i in range(1,100) ])
        self.save = {}
        self.vg = vgmanager
        #os.system("rmmod drbd")
        os.system("modprobe drbd minor_count=100 usermode_helper=/bin/true")
        
    def get_free_drbd(self):
        """
        return a free drbd device.
        """
        tmp = [ k for k, v in self.drbds.items() if not v]
        if not tmp: raise Exception, "No more drbd available"
        return tmp[0]
    
    def reverse(self, path):
        """
        Get the drbd from the original path device
        """
        reverse = dict([ (v, k) for k, v in self.drbds.items() ])
        return reverse[path]
    
    def resolve(self, path):
        """
        LVM manage his device via symlink, use this function to find the real DM device.
        """
        return os.readlink(path)
    
    def get_path_size(self, path):
        """
        Return the size of path in octets
        """
        return long(execute("/sbin/blockdev --getsize64 '%s'"%path, True, timeout=5).strip())
    
    def get_path_sector(self, path):
        """
        Return the size of path in sector. FIXME : sector are harcoded to 512 octets
        """
        return self.get_path_size(path)/512
    
    def calcport(self, drbd):
        """
        Get an unique port number depending of the drbd device used
        """
        return self.BASEPORT + int(drbd.replace("drbd", ""))
    
    def create(self, path):
        """
        Create a drbd to use with lv path. Register it in self.drbds

        Return the unique port to use with the created drbd.
        """
        size = self.get_path_size(path)
        drbd = self.get_free_drbd()
        self.drbds[drbd] = path
        realpath = self.resolve(path)
        metapath = path + "-drbdmeta"
        metaname = os.path.basename(metapath)
        metasize = max(((size/2**18.0)*8+72) * 512, 128*1024*1024)
        self.vg.lv_create(metaname, metasize)
        realmetapath = self.resolve(metapath)
        copypath = realpath + "--copy-for-drbd"
        copyname = os.path.basename(copypath)
        table = execute("dmsetup table %s"%realpath, True, timeout=10)
        # On creer un peripherique qui est une copie de l'ancien pour pouvoir restaurer ensuite
        execute("dmsetup create %s"%(copyname), True, timeout=20, tosend=table)
        execute("modprobe drbd minor_count=100 usermode_helper=/bin/true", timeout=10)
        file(realmetapath, "w").write("\0"*262144)
        execute("drbdmeta -f /dev/%s v08 %s 0 wipe-md"%(drbd, realmetapath), timeout=20)
        execute("drbdmeta -f /dev/%s v08 %s 0 create-md"%(drbd, realmetapath), True, timeout=20)
        execute("drbdsetup /dev/%s disk %s %s 0 --create-device"%(drbd, copypath, realmetapath), True, timeout=20)
        return self.calcport(drbd)
    
    def connect(self, path, remote, remoteport, rate=50000):
        """
        Connect the drbd associated with path the the host remote:remoteport.

        Limit rate to rate once connected.
        """
        drbd = self.reverse(path)
        execute("drbdsetup /dev/%s net %s:%s %s:%s C -m -S 10000000"%(drbd, ip, self.calcport(drbd), remote, remoteport), True, timeout=20)
        time.sleep(3)
        execute("drbdsetup /dev/%s syncer -r %s"%(drbd, rate), timeout=10)
        
    def primary(self, path):
        """
        Switch the drbd associated with path to primary mode
        """
        execute("drbdsetup /dev/%s primary -o"%self.reverse(path), timeout=10)
        
    def secondary(self, path):
        """
        Switch the drbd associated with path to secondary mode
        """
        execute("drbdsetup /dev/%s secondary"%self.reverse(path), timeout=10)
        
    def switch_to_drbd(self, path):
        """
        Switch the device designated by path on the associated drbd.
        """
        sectors = self.get_path_sector(path)
        realpath = self.resolve(path)
        print execute("dmsetup load %s --table '0 %d linear /dev/%s 0'"%(realpath, sectors, self.reverse(path)), True, timeout=10)
        execute("dmsetup suspend %s "%realpath, True, timeout=10)
        execute("dmsetup resume %s "%realpath, True, timeout=10)
        
    def switch_from_drbd(self, path):
        """
        Switch back the device designated by path to real device.
        """
        realpath = self.resolve(path)
        copypath = realpath + "--copy-for-drbd"
        table = execute("dmsetup table %s"%copypath, True, timeout=10)
        execute("dmsetup load %s"%(realpath), True, timeout=10, tosend=table)
        execute("dmsetup suspend %s "%realpath, True, timeout=10)
        execute("dmsetup resume %s "%realpath, True, timeout=10)
        
    def destroy(self, path):
        """
        Destroy the drbd and release all the ressource allocated for him.
        """
        drbd = self.reverse(path)
        realpath = self.resolve(path)
        copypath = realpath + "--copy-for-drbd"
        metaname = os.path.basename(path) + "-drbdmeta"
        execute("drbdsetup /dev/%s disconnect"%drbd, timeout=20)
        execute("drbdsetup /dev/%s down"%drbd, timeout=20)
        execute("dmsetup remove %s"%copypath, timeout=10)
        self.vg.lv_remove(metaname)
        self.drbds[drbd] = None
        
    def wait_sync(self, path):
        """
        Wait for syncronisation on the drbd associated with path
        """
        drbd = self.reverse(path)
        result = execute("drbdsetup /dev/%s wait-sync"%drbd)
        

class Host(object):
    """
    Class to manage the Host. Have to autodetect the ressource container of
    the current server (VM, disk, etc)
    """
    CONFDIR = [ i for i in ("/etc/xen/auto", "/etc/xen/noauto") if os.path.isdir(i) ]
    def __init__(self):
        xenversion = "%s.%s"%(file("/sys/hypervisor/version/major").read().strip(),
                                file("/sys/hypervisor/version/minor").read().strip())
        xenextra = "%s%s"%(xenversion, file("/sys/hypervisor/version/extra").read().strip())
        for p in (xenversion, xenextra):
            xenpath = "/usr/lib/xen-%s/lib/python"%p
            if xenpath not in sys.path: sys.path.append(xenpath)
        from xen.xm.XenAPI import Session
        session = Session('httpu:///var/run/xend/xen-api.sock')
        session.login_with_password(' ', ' ')
        self.api = session.xenapi
        # self.api.host.list_methods()
        self.metrics = XenMetrics(self.api)
        self.vg = VGManager.guess_managers()
        self.drbd = DRBDManager(self.vg.values()[0])
        self.name = hostname # s'pas propre la globale.
        self.busy = False
        self._allocatable = True
        
    def _vm_from_config(self, pattern=None):
        """
        Return a hash of instancied VM using configfile
        """
        result = {}
        for vmname in reduce(lambda a, b: a+b,[ os.listdir(d) for d in self.CONFDIR ]):
            if vmname and ( vmname [0] in ".#" or vmname.endswith("~") ):
                continue
            if not pattern or fnmatch(vmname, pattern):
                result[vmname] = VM(self, name=vmname)
        return result
    
    def _vm_from_xenapi(self, pattern=None):
        """
        Return a hash of instancied VM using xenapi
        """
        result = {}
        for vm_rec in self.api.VM.get_all():
            tmp = VM(self, uuid = vm_rec)
            if not pattern or fnmatch(tmp.name, pattern):
                result[tmp.name] = tmp
        return result
    
    def _vm_list(self, pattern = None):
        """
        Return a list of all available vm, started or not
        """
        tmp = self._vm_from_config(pattern)
        tmp.update(self._vm_from_xenapi(pattern))
        return tmp
    
    def get_cpu_info(self, cpuuuid):
        """
        Get cpu information on host
        """
        tmp = {}
        cpu_record = self.api.host_cpu.get_record(cpuuuid)
        tmp["cpuname"] = cpu_record["modelname"]
        tmp["cpu_usage"] = cpu_record["utilisation"]
        tmp["cpu_freq"] = cpu_record["speed"]
        return tmp
    
    def get_device_stats(self, delay=4):
        """
        Retrieve stats for network and disk over 4 seconds.
        """
        result = {}
        tt = time.time()
        ethx = re.findall(" *(?P<name>eth[0-9]+):(?P<receive>[0-9]+) +([0-9]+ +){7}(?P<transmit>[0-9]+) ",
                          file("/proc/net/dev").read())
        disks = re.findall("(?P<name>(sd|hd)[a-z]+) [0-9]+ [0-9]+ (?P<read>[0-9]+) [0-9]+ [0-9]+ [0-9]+ (?P<write>[0-9]+)",
                           file("/proc/diskstats").read())
        time.sleep(delay)
        ethx2 = re.findall(" *(?P<name>eth[0-9]+):(?P<receive>[0-9]+) +([0-9]+ +){7}(?P<transmit>[0-9]+) ",
                          file("/proc/net/dev").read())
        disks2 = re.findall("(?P<name>(sd|hd)[a-z]+) [0-9]+ [0-9]+ (?P<read>[0-9]+) [0-9]+ [0-9]+ [0-9]+ (?P<write>[0-9]+)",
                            file("/proc/diskstats").read())
        delta = time.time() - tt
        # On pepare les donnees.
        ethx2 = dict([ (name,(long(receive),long(transmit))) for name, receive, junk, transmit in ethx2 ])
        result["interface"] = {}
        for name, receive, junk, transmit in ethx:
            result["interface"][name] = {}
            result["interface"][name]['receive'] = (ethx2[name][0] - long(receive)) / delta
            result["interface"][name]['transmit'] = (ethx2[name][1] - long(transmit)) / delta
        disks2 = dict([ (name, (long(read), long(write))) for name, junk, read, write in disks2])
        result["disk"] = {}            
        for name, junk, read, write in disks:
            result["disk"][name] = {}
            result["disk"][name]["read"] = (long(disks2[name][0]) - long(read)) / delta * 512
            result["disk"][name]["write"] = (long(disks2[name][1]) - long(write)) / delta * 512
        return result
    
    def info(self, stats=False):
        """
        Get host information. If stats=True, then gather statistics as well.
        """
        result = {}
        host_ref = self.api.host.get_all()[0] # on suppose qu'on a qu'un seul hote ...
        result["uuid"] = host_ref
        host_record = self.api.host.get_record(host_ref)
        result["hostname"] = hostname
        result["ip"] = ip
        result["name"] = host_record["name_label"]
        result["xen_version"] = host_record["software_version"]["Xen"]
        result["kernel_version"] = host_record["software_version"]["release"]
        result["xend_version"] = host_record["software_version"].get("Xend", "None")
        result["xenocontrol_version"] = VERSION
        result["enabled"] = host_record["enabled"] # si desactive alors on ne peut pas creer de vm
        result["capabilities"] = host_record["capabilities"] # si desactive alors on ne peut pas creer de vm
        result["nr_vm"] = len(host_record["resident_VMs"]) # Nombre de vm.
        result["hvm"] = bool([ i for i in result["capabilities"] if "hvm" in i ])
        result["cpu"] = int(host_record["cpu_configuration"]["nr_cpus"])
        result["cputhread"] = result["cpu"] * int(host_record["cpu_configuration"]["threads_per_core"])
        metrics = self.api.host_metrics.get_record(host_record["metrics"])
        result["mem"] = int(metrics["memory_total"])
        result["memMB"] = result["mem"]/1024/1024
        result["memfree"] = int(metrics["memory_free"])
        result["memfreeMB"] = result["memfree"]/1024/1024
        result["cpus"] = [ self.get_cpu_info(c) for c in host_record["host_CPUs"] ] # le temps report la dedans est foireux.
        result["cpu_usage"] = sum([ sum(self.api.VM_metrics.get_VCPUs_utilisation(self.api.VM.get_metrics(i)).values())
                                    for i in host_record["resident_VMs"] ])
        result["busy"] = self.busy
        result["allocatable"] = self._allocatable
        if stats:
            tmp = self.get_device_stats()
            result["disk"] = tmp["disk"]
            result["interface"] = tmp["interface"]
        #FIXME : la on rapporte juste 1 seul disque
        result.update(self.vg.values()[0].vg_info())
        return result

    def stats(self):
        """
        Wrapper for self.info(stats=True)
        """
        return self.info(stats=True)
    
    def lv_list(self):
        """
        List all lv on host
        """
        result = self.vg.values()[0].lv_list()
        for tmp in result:
            tmp["host"] = self.name
        return result
    
    def mdstat(self):
        """
        Get /proc/mdstat information
        """
        data = file("/proc/mdstat").read()
        mdlist = re.findall(r"(md[0-9]) .*(raid[^ ]*).*\n.*([[][^ ]*[]])\n(.*(resync.+)\n)?", data)
        result = [ "%s : %s %s %s %s"%(self.name, d, t, u, r) for d, t, u, junk, r in mdlist ]
        result.sort()
        return result
    
    def date(self):
        """
        Return current date.
        """
        return time.ctime()
    
    def ntp_restart(self):
        """
        Helper to restart ntp
        """
        return execute("/etc/init.d/ntp restart", 5)
    
    def execute(self, cmd, timeout):
        """
        Helper to execute arbitrary cmd on host. Can be desactivated by setting EXECUTE_ENABLED to false.
        """
        if EXECUTE_ENABLED:
            return execute(cmd, timeout=timeout)
        else:
            return "You cannot execute anything on this host"

    def can_host(self, cpu, memory, disk, hvm=False):
        """
        Return our hostname if the current host have all the ressource available.
        """
        if not self._allocatable: return
        if self.busy: return 
        tmp = self.info()
        maxcpu = len(tmp["cpus"])
        if tmp["cpu_usage"] + cpu > maxcpu: return 
        if tmp["memfreeMB"] < memory: return 
        if tmp["storage_freeGB"] < disk: return 
        if hvm and not tmp["hvm"]: return 
        return self.name
    
    def can_install(self, templatename):
        """
        Specific to installsystem. Return true if this installation type exist.
        """
        # FIXME : Les checks sont un peu leger pour l'instant
        if os.path.isdir("/root/installsystems/%s/"%templatename): return self.name
        else: return
        
    @lock("install")
    def install(self, templatename, hostname, args):
        """
        Install new host (by using installsystem scripts)
        """
        self.busy = True
        print "Received an installation order for %s (%s)"%(hostname, repr(args))
        if "dummy" in args: return CODE_OK
        params = " ".join([ "--%s=%s"%(k, v) for k, v in args.items() ])
        try:
            result = execute("cd /root/installsystems ; ./install.py %s -n %s %s"%(templatename, hostname, params), raiseonfailure=True, timeout=3600)
            return CODE_OK
        except:
            return CODE_KO
        finally:
            self.busy = False
            
    def set_busy(self, value=False):
        """
        Set the busy flag on the host.
        """
        self.busy = value
        return CODE_OK
    
    def is_busy(self):
        """
        Return hostname if busyflag is set.
        """
        if self.busy:
            return self.name
        
    def is_available(self):
        """
        Return hostname if busyflag is not set.
        """
        if not self.busy:
            return self.name
        
    @lock("hostallocatable")
    def allocatable(self, value=True):
        """
        Set the allocatable flags to value. FIXME : Used ?
        """
        self._allocatable = value
        return CODE_OK
    
    def xend_restart(self):
        """
        Restart xend. Sometime necessary as xend is buggy and stop gather statistics.
        Be aware that times to times, xend is not able to restart.
        """
        return execute("/etc/init.d/xend restart", timeout=30)
    
    def create_vmconfig(self, config, confpath):
        """
        Create new config file for VM.
        FIXME : should be in VMManager
        """
        try:
            file(confpath, "w").write(config)
            return CODE_OK
        except:
            return CODE_KO

class Processor(object):
    """
    The class that wrap the spread connection and communicate over the network.
    """
    def __init__(self, name="xenhost", group=SPGRP):
        try:
            self.spread = spread.connect(name=name)
        except spread.error,e:
            print "Sorry, we've got this error while trying to connect to spread:"
            print "  %s"%e[1]
            print "\nPlease verify that your spread daemon is running and accessible (with spuser)\n"
            sys.exit(1)
        self.members = {}
        self.serverlist = set([])
        self.spreadname = self.spread.private_group
        self.replyname = self.spreadname # Destinataire des reponses ! Si on met un group on doit etre dedans !
        self.name = self.spreadname
        self.group = group
        self.join_group(group)
        
    def __del__(self):
        try: self.spread.disconnect()
        except: pass
        
    def join_group(self, name):
        """
        Join a spread group. A spread group is a little like a IRC channel ;
        everyone that have joined receive the message adressed to it.
        """
        self.spread.join(name)
        self.members[name] = set([])
        # When we join, we receive e message telling us that we've join. Process this message now.
        self.receive()
        
    def fileno(self):
        """
        Return the fileno associated with the network socket, to use with select or poll.
        """
        return self.spread.fileno()
    
    def receive(self, block=True):
        """
        Receive some new data, and call handle_<xxxx> depending of the message type.
        If called with block=False, verify that there is something to read so we don't
        block indefinitly in a syscall.
        """
        if not block:
            if not select.select([self.spread], [], [], 0.1)[0]:
                return
        msg = self.spread.receive()
        if isinstance(msg, spread.MembershipMsgType):
            return self.handle_member(msg)
        elif isinstance(msg, spread.RegularMsgType):
            if msg.sender == self.spreadname: return None
            if msg.msg_type in RCONTENT_TYPE:
                return getattr(self, "handle_%s"%RCONTENT_TYPE[msg.msg_type].lower(), self.handle_msg)(msg)
            else:
                return self.handle_msg(msg)
        else:
            raise "Uncatched type of event"
        
    def send(self, message, simulate = False):
        """
        Send 'message' to the network. Message have to be a Reply object.
        If simulate=True, then just simulate ... :)
        """
        if not message.sender: message.sender = self.replyname
        sended = set([])
        for msg in message.to_network():
            #debug(("Sending %s"%repr(msg))[:75])
            if msg[1] == SPGRP: sended.update(self.serverlist)
            else: sended.add(msg[1]) # .lower() ?
            if not simulate:
                self.spread.multicast(*msg)
        message.set_sended(sended)
        
    def handle_member(self, msg):
        """
        Handle a 'membership' message in spread, used to signal join/leave in a group.
        """
        # Msg have those attributes.
        # ['extra', 'group', 'group_id', 'members', 'reason']
        self.members[msg.group] = set(msg.members)
        if msg.group == self.group: 
            self.serverlist = set([ i for i in msg.members if "xenhost" in i ])
            
    def handle_msg(self, msg):
        """
        Default handling function for spread message.
        """
        # Msg have those attributes
        # ['endian', 'groups', 'msg', 'msg_type', 'sender']
        print "Received %s from %s (type %s)"%(repr(msg.message)[:50], msg.sender, RCONTENT_TYPE.get(msg.msg_type, None))
        
    def handle_error(self, msg):
        """
        Handle spread message of error type.
        """
        for error in Error.from_network(msg):
            print error
            
    def handle_request(self, msg):
        """
        Handle spread message of request type.
        Should be overriden in subclass
        """
        return msg
    
    def handle_reply(self, msg):
        """
        Handle spread message of reply type.
        Should be overriden in subclass
        """
        return msg
    
    def handle_automation_ack(self, msg):
        """
        Handle spread message of automation_ack type.
        Should be overriden in subclass
        """
        return msg


class WorkerThread(threading.Thread):
    """
    Helper class to manage task that must be executed by the xenocontrol
    server in separate thread, so it don't block other request on the cluster.
    """
    def __init__(self, processor, request, tasklist):
        """
        processor is the xenprocessor instance,
        request is the request object
        tasklist is a list of task that have to be executed in sequence.
        """
        super(WorkerThread, self).__init__()
        self.processor = processor
        self.request = request
        self.tasklist = tasklist
        self._status = StatusTracker(name=self.request.query[1], initialdata="Request : %s"%self.request)
        self.start()

    def set_status(self, data):
        """
        Set a status, tracking it in the local StatusTracker instance.
        """
        self._status.set(data)
        
    def get_status(self):
        """
        Return the current status.
        """
        return self._status.get()
    
    status = property(get_status, set_status)
    
    def run(self):
        """
        Execute the WorkerThread.
        You normaly don't have to call run manually, it should be done
        by the __init__ constructor (via self.start())
        """
        self.processor.workerlist.append(self)
        self.status = "Executing function"
        reply = self.request.respond()
        i = 0
        for identifier, func, kv in self.tasklist:
            if not kv: kv = {}
            i += 1
            try:
                self.status = "Processing task %s"%i
                if hasattr(func, "can_notify"):
                    reply += self.request.respond(buildresult(func(self.notify, **kv), identifier))
                else:
                    reply += self.request.respond(buildresult(func(**kv), identifier))
            except Exception, e:
                self.status = "In Error for task %s"%i
                error = "Your call (%s, %s) on %s raise an exception : %s : %s"%(func, repr(kv), identifier, e, compact_traceback())
                self.processor.send(self.request.rerror(error))
        self.status = "Sending Reply"
        self.processor.send(reply)
        self.status = "Dying"
        self.processor.workerlist.remove(self)
        
    def __str__(self):
        return "%s : %s"%(self.request.query, self.status)
    
    def notify(self, data):
        """
        Notify a status. In fact, update the status. FIXME : to remove ?
        """
        self.status = data


class XenProcessor(Processor):
    """
    Subclass of processor to handle the 'daemon' side of xenocontrol
    """
    def __init__(self, *l, **kv):
        Processor.__init__(self, *l, **kv)
        self.workerlist = []
        self.host = Host()
        self.reserved = False
        self.automate = AutomationHelper(self)
        
    def handle_request(self, msg):
        """
        Handle msg of request type.
        
        Extract the rpc call requested from the request, and fill a WorkerThread
        with the appropriate info to make the call.

        Note that we try to guess a lot of things here, this should be cleaned up.
        """
        # Msg have those attributes
        # ['endian', 'groups', 'message', 'msg_type', 'sender']
        print "got request %s"%msg.message
        error = []
        try:
            request = Request.from_network(msg)
            (target, funcname, kv) = request.query
            selector = request.selector
            sequence = request.sequence
        except Exception, e:
            self.send(Error("%s"%e))
            return
        if "host" in selector and not fnmatch(hostname, selector["host"]):
            self.send(request.respond())
            return 
        elif funcname.startswith("_"):
            error.append("You cannot call a (private) method starting with _")
        elif target.lower() == "host":
            f = getattr(self.host, funcname, None)
            if f:
                WorkerThread(self, request, [(hostname, f, kv)])
            else: error.append("Unknow method %s for %s"%(funcname, target))
        elif target.lower() == "vm":
            tasklist = []
            for vm in self.host._vm_list(selector.get("vm", "__NO_MATCH__")).values():
                f = getattr(vm, funcname, None)
                if f:
                    tasklist.append((vm.name, f, kv))
                else: error.append("Unknow method %s for %s"%(funcname, target))
            WorkerThread(self, request, tasklist)
        elif target.lower() == "internal":
            f = getattr(self, funcname, None)
            if f and funcname in ( "update", "version", "tasks", "stop_automation"):
                if funcname == "update": f(**kv) # Cas particulier quand on se reexec ...
                WorkerThread(self, request, [(hostname, f, kv)])
            else: error.append("Unknow method %s for %s"%(funcname, target))
        elif target.lower() == "automation":
            # In this case, funcname is the name of the automation
            try:
                if "name" not in kv: raise Exception("When starting a automation, you have to give a name in the parameters")
                result = self.start_automation(funcname, **kv)
                self.send(request.respond(buildresult(result, hostname)))
            except Exception, e:
                errormsg = "Your call (%s, %s) raise an exception : %s : %s"%(funcname, repr(kv), e, compact_traceback())
                self.status = "Exception while starting automation %s(%s) : %s"%(funcname, kv, errormsg)
                error.append(errormsg)
        else: error.append("Unknow target type %s"%target)
        if error:
            self.send(request.rerror(error))
            self.send(request.respond())
            
    def execute_task(self, cmdqueue):
        """
        In a multi-thread program, you need to fork in the main thread.

        The purpose of this method is to satisfy this constraint ; as rpc are executed in
        a WorkerThread instance, every execute command must be placed in a queue so the
        main thread can fork and exec it.
        """
        try:
            while True:
                cmd, reply_channel = cmdqueue.get_nowait()
                # FIXME : rajouter un timeout ?
                if cmd:
                    reply = Popen(cmd, shell=True, 
                                  stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
                    reply_channel.put(reply)
                else:
                    reply_channel.put(None)
        except Empty:
            pass
        
    def serve(self):
        """
        Serve request until global quit is set to True.
        """
        global quit, cmdqueue
        quit = False
        while not quit:
            li, lo, le = select.select([self],  [], [], 0.1)
            for i in li:
                print i.receive()
                print i.serverlist
            if not cmdqueue.empty(): self.execute_task(cmdqueue)
            
    def version(self):
        """
        Return version.
        """
        return "%s : %s"%(hostname, VERSION)
    
    def update(self, version, code):
        """
        Update current code with code provided in parameters if provided version
        is more recent that ours, the reexecute ourselve to load new code.
        """
        if version > VERSION:
            # TODO rajouter checksum et signature
            if not code: return CODE_KO
            try: compile(code, "test", "exec")
            except: return CODE_KO
            filename = globals()["__file__"]
            file(filename, "w").write(code)
            self.spread.disconnect()
            debug("Receive update (%s to %s), reexecuting myself"%(VERSION, version))
            time.sleep(2)
            os.execv(filename, sys.argv)
        else:
            return "%s : I'm already using this version"%hostname
        
    def tasks(self):
        """
        List tasks TODO : this things just sucks for now.
        """
        return [ a._status.serialize() for a in self.workerlist + self.automate.automatelist if a._status.name != "tasks" ]
        #return [ a for a in ( "%s"%a for a in self.workerlist ) if "'tasks'" not in a ] + [ "%s"%a for a in self.automatelist ]
        
    @lock("ProcessorLock")
    def reserve_host(self, data=True):
        """
        Reserve the host
        """
        if not self.reserved:
            self.reserved = data
            return CODE_OK
    @lock("ProcessorLock")
    def release_host(self):
        if self.reserved:
            self.reserved = False
            return CODE_OK
    def start_automation(self, *l, **kv):
        self.automate.start_automation(*l, **kv)
    def stop_automation(self, *l, **kv):
        self.automate.stop_automation(*l, **kv)

class AutomationHelper(object):
    def __init__(self, processor):
        self.processor = processor
        self.automatelist = []
    def start_automation(self, automation, name, **kv):
        global Automations
        if automation not in Automations:
            raise Exception, "%s : No such automation !"%automation
        a = Automations[automation](name)
        # TODO : Find a way to handle *very* slow server. If our master take more that
        # timeout seconds to initiate his automation, it'll conflict with 2nd node of
        # the list (which have been elected as master)
        a.prepare_automation(num = len(self.processor.serverlist), timeout=20)
        t = threading.Thread(target=a.run_threaded, args=(self,), kwargs=kv)
        t.name = name
        t.start()
        return CODE_OK
    def stop_automation(self, name):
        pprint(threading.enumerate())
        i = 0
        for automate in self.automatelist[:]:
            if automate.name == name:
                automate.quit = True
                i += 1
        return i

#-------------------------------------------------------------------------------
#
# END Servers Stuff
#
#-------------------------------------------------------------------------------


################################################################################
#
# CLIENT's stuff
#
################################################################################


class ClientProcessor(Processor):
    def __init__(self, name=None, group=SPGRP):
        if not name:
            name = "".join(random.sample("abcdefghijklmnopqrstuvwxyz",12))
        Processor.__init__(self, name=name, group=group)
    def handle_request(self, msg):
        # On est client, on discard toute les requetes recues
        pass
    def build_query(self, **kv):
        """ Build the query by looking for the keyword target, funcname, param """
        if "target" not in kv: kv["target"] = hasattr(self, "target") and self.target or "Host"
        if "funcname" not in kv: kv["funcname"] = hasattr(self, "funcname") and self.funcname or self.__class__.__name__.lower()
        if "param" not in kv: kv["param"] = {}
        return (kv["target"], kv["funcname"], kv["param"])
    def selector_from_pattern(self, pattern, default="host"):
        if "/" in pattern:
            hostpattern, vmpattern = pattern.split("/", 1)
            selector = { "host": hostpattern,
                         "vm": vmpattern }
        else:
            selector = { default.lower() : pattern }
        return selector

class SingleRequestProcessor(ClientProcessor):
    sortkeys = []
    def collect_replies(self, request):
        """ Send the request and return all replies. A reply is of the form """
        self.send(request)
        resp = 0
        values = []
        collected = set()
        while resp < len(self.serverlist):
            tmp = self.receive()
            if tmp:
                resp += 1
                #try:
                reply = Reply.from_network(tmp)
                if request.sequence == reply.sequence:
                    values += reply.message
                    debug("Got a reply from %s"%reply.sender)
                else:
                    debug("Invalid sequence number from %s (got %s expected %s)"%(reply.sender, request.sequence, reply.sequence))
                    resp -= 1 # Non prevue donc on annule
                #except:
                #    debug("Got an invalid reply from %s"%tmp.sender)
        return values
    def parse_replies(self, replies, options):
        result = []
        tmp = self.reply_headers(replies, options)
        if tmp: result.append(tmp)
        total = { "reply_count": 0 }
        for reply in replies:
            total = self.sum_replies(total, reply, options)
            result.append(self.format_replies(reply, options))
        end = self.end_replies(total, options)
        if end: result.append(end)
        return "\n".join(result)
    def sort_replies(self, replies, options):
        """ This function can sort replies if need. It have to modify the list in place """
        if self.sortkeys:
            def f(a):
                return [ a[i] for i in self.sortkeys ]
            replies.sort(key=f)
        else:
            replies.sort()
    def sum_replies(self, total, reply, options):
        if isinstance(reply, dict):
            reply["reply_count"] = reply.get("reply_count", 0) + 1
            for k, v in reply.items():
                if isinstance(v, int) or isinstance(v, long) or isinstance(v, float):
                    total[k] = total.get(k, 0) + v
        return total
    def run(self, selector, **options):
        query = self.build_query(param=options)
        request = Request(query = query, selector = selector)
        replies = self.collect_replies(request)
        self.sort_replies(replies, options)
        return self.parse_replies(replies, options)
    def parse_options(self, *l, **kv):
        raise NotImplementedError("this function *have* to be override by userinterface classes")
    def format_replies(self, reply, options):
        raise NotImplementedError("this function *have* to be override by userinterface classes")
    def end_replies(self, total, options):
        raise NotImplementedError("this function *have* to be override by userinterface classes")
    def reply_headers(self, replies, options):
        raise NotImplementedError("this function *have* to be override by userinterface classes")


###########################################################################################
# The User interface.
# Those classes respond to the API of SingleRequestProcessor to 
#  1/ Interpret various parameters of user provided input method
#  2/ Format replys to the appropriate format for use by frontend
###########################################################################################


class Frontend(object):
    def parse_options(self, *l, **kv):
        raise NotImplementedError("this function *have* to be override by userinterface classes")
    def parse_and_run(self,*l, **kv):
        selector, options = self.parse_options(*l, **kv)
        return self.run(selector=selector, **options)
    def format_replies(self, reply, options):
        pass
    def end_replies(self, total, options):
        pass
    def reply_headers(self, replies, options):
        pass


Cmds = {}
class CmdLine(Frontend):
    __metaclass__ = AutoDict(Cmds, condition=lambda a: hasattr(a, "target") and "Cmd" not in a.__name__)
    options = [ atot("-s", "--sortkeys", help="Set sort order, use comma to give more than one.", dest="sortkeys") ]
    fmt = "%(identifier)s : %(result)s"
    helptext = "No help available on this action. If you don't know what it does,\n you don't want to use it."
    def __init__(self, *l, **kv):
        if not hasattr(self, "funcname") and hasattr(self, "target"):
            name =  self.__class__.__name__.lower()
            self.funcname = name.replace(self.target.lower(), "")
        super(CmdLine, self).__init__(*l, **kv)
    def selector_from_pattern(self, pattern, default=None):
        return super(CmdLine, self).selector_from_pattern(pattern, getattr(self, "target", default))
    def OptionParser(self):
        """ OptionParser build an OptionParser instance with desired option"""
        parser = OptionParser(usage=self.usage())
        for l, kv in self.options:
            parser.add_option(*l, **kv)
        return parser
    def print_help_and_exit(self):
        self.OptionParser().print_help()
        sys.exit(1)
    def parse_options(self, *l, **kv):
        (options, args) = self.OptionParser().parse_args()
        if len(args) > 1: pattern = args[1]
        else: pattern = hasattr(self, "defaultpattern") and self.defaultpattern or "*"
        if pattern == "__NOMATCH__": self.print_help_and_exit()
        selector = self.selector_from_pattern(pattern)
        options = options.__dict__.copy()
        if "sortkeys" in options:
            if options["sortkeys"]:
                self.sortkeys = [ a.strip() for a in options["sortkeys"].split(",") ]
            del options["sortkeys"]
        return (selector, options)
    def format_replies(self, reply, options):
        if hasattr(self, "fmt"):
            return self.fmt%reply
        else:
            return str(reply)
    def end_replies(self, total, options):
        """ Method used to print total eventually collected by calls to format_replies """
        return "%s"%total
    def reply_headers(self, replies, options):
        if len(replies) > 0 and hasattr(self, "fmt") and type(replies[0]) is dict:
            z = replies[0].keys()
            head = dict(zip(z, z))
            return self.fmt%head
    def usage(self):
        result = []
        name = self.__class__.__name__.lower()
        result.append("\n  %prog <action> selectorpattern [options]")
        result.append("\n%prog is a distributed cloud manager designed for xen.")
        result.append("")
        result.append(textwrap.dedent("""\
        Selector Pattern:
          Should be use to restrict/select a set of HOSTs or VMs on which you want to
          execute your action. The selector is made of two parts : one for the HOST, and the
          other for the VM, with the following syntax : HOST/VM .  You could use shell-like
          pattern matching one each part of the pattern. Eg:
            - xen-1/vm-1 will match the vm named 'vm-1' on host 'xen-1'
            - xen-1/* will match all the vm on host 'xen-1'
            - */vm-1 will match all the vm named 'vm-1'
            - */*-1 will match all the vm which have their name ending with '-1'
            - xen-{1,2}/* will match all the vm running on host xen-1 and xen-2
          If you omit the / in the selector, the resulting pattern depends of the action :
            - for command starting with host, pattern 'toto' will translate to 'toto/*'
            - for command starting with vm, pattern 'titi' will translate to '*/titi'
          The default pattern depends of the requested action ; it'll not match anything for
          harmful (like vmdelete) command and match everything for safe command (like vmlist).
        """))
        if name == "cmdline":
            # No action specified, listing them all.
            result.append("\nAvailable Actions:")
            commands = Cmds.items()
            commands.sort()
            for c,v in commands:
                result.append("  %s : %s"%(c.ljust(20), v.helptext.split("\n")[0]))
        else:
            # We have an action specified, display the associated helptext
            result.append("\nAction %s:\n%s"%(
                name,
                "\n".join([ "  %s"%line for line in self.helptext.split("\n")])
                ))
        return "\n".join(result)



class HTMLProcessor(Frontend):
    pass

###########################################################################################
# Les automations !
###########################################################################################

Automations = {}
class AutomationProcessor(ClientProcessor):
    __metaclass__ = AutoDict(Automations, condition=lambda a: "Processor" not in a.__name__)
    # Automation Helper
    def __init__(self, name="DebugAuto"):
        self.workers = {}
        self.started = False
        # We unify random on all node. Use self.random and not random in your code.
        # hash don't give the same output on 32 and 64 bits so we don't use it.
        self.random = random.Random(sum([ ord(c) for c in name ]))
        self.lastrequest = None
        self._status = StatusTracker(name)
        self.notify = None
        self.requestcount = 0
        self.ackcount = 0
        self.master = None
        self.quit = False
        super(AutomationProcessor, self).__init__(name = name)
        self.name = name # On force le automatename au groupe comme ca tout le monde peut ecouter ce qui se passe
        self.replyname = self.name
        self.join_group(name)
        debug("Automation %s created"%(self.spreadname))
    def __str__(self):
        return "%s  Type : %s  Identifier : %s  Status : %s"%(self.is_master() and "MASTER" or "SLAVE", self.__class__.__name__, self.name, self.status)
    def prepare_automation(self, num=-1, timeout=5):
        """ This method call receive for a certain amount of time so the Processor call member_message """
        while timeout > 0:
            t = time.time()
            tmp = self.receive(block=False)
            if tmp: debug("Aye. We're not supposed to receive anything here ... : %s"%tmp)
            timeout -= time.time() - t
            if len(self.automate_members()) == num:
                debug("We have all our members (%s) now, let's start"%num)
                break
        return True
    def automate_members(self):
        return [ m for m in self.members[self.name] if m.startswith("#"+self.name) ]
    def set_master(self):
        master = min(self.automate_members())
        debug("Setting master from %s to %s (candidate : %s)"%(self.master, master, self.automate_members()))
        self.master = master
        debug("Are we master now ? %s"%self.is_master())
    def is_master(self):
        return self.spreadname == self.master
    def get_status(self):
        return self._status.get()
    def set_status(self, value):
        if self.notify:
            self.notify(value)
        print "New status for automate %s : %s"%(self.spreadname, value)
        self._status.set(value)
    status = property(get_status, set_status)
    def add_worker(self, worker):
        self.send_to_worker(worker)
    def send_to_worker(self, worker, reply = None):
        if not self.master: self.set_master()
        try:
            if reply is not None:
                request = worker.send(reply)
            else:
                request = worker.next()
            if request: # Si on a une requete ... Sinon le generateur doit vouloir collecter des datas de plusieurs hotes
                # On override le numero de sequence pour qu'il soit le meme sur tout les hotes
                self.requestcount += 1
                request.sequence = "%s-%s"%(self.name, self.requestcount)
                if self.is_master():
                    debug("Master is sending request & ack for %d"%self.requestcount)
                    self.send(request)
                    self.send(request.ack(self.name))
                else:
                    debug("We're slave, not sending anything ...")
                    self.send(request, simulate=True)
                    self.lastrequest = request
                self.workers[worker] = request
        except StopIteration:
            del self.workers[worker]
    def handle_reply(self, msg):
        reply = Reply.from_network(msg)
        tmp = [ worker for worker, request in self.workers.items() if reply.sequence == request.sequence ]
        if not tmp:
            print "received an unregistered sequence number, not for us ! %s not in %s"%(
                reply.sequence, [ request.sequence for request in self.workers.values() ])
            return
        worker = tmp[0]
        self.send_to_worker(worker, reply)
        return msg
    def handle_automation_ack(self, msg):
        self.ackcount += 1
        debug("Ack for command %d received"%self.ackcount)
        return msg
    def handle_member(self, msg):
        """ Msg have those attributes.
        ['extra', 'group', 'group_id', 'members', 'reason']
        """
        super(AutomationProcessor, self).handle_member(msg)
        for worker, request in self.workers.items():
            if not request.sended.issubset(self.serverlist):
                missinghost = (request.sended - self.serverlist).pop()
                print "request %s not valid anymore due to disconnect of %s"%(request, missinghost)
                reply = request.respond() # genere une reponse vide pour l'hote deconnecte.
                reply.sender = missinghost
                self.send_to_worker(worker, reply)
                request.sended.remove(missinghost)
        if self.started and msg.group == self.name and self.master not in self.members[self.name]:
            debug("We just loose our master, electing a new one !")
            self.set_master()
            if self.is_master() and self.ackcount != self.requestcount:
                # The failing node didn't send his command, we do it for him !
                debug("The failing node didn't send his command, we do it for him !")
                self.send(self.lastrequest)
                self.send(self.lastrequest.ack(self.name))
    def run_threaded(self, control, **kv):
        control.automatelist.append(self)
        if "stored_selector" in kv:
            kv["selector"] = kv["stored_selector"]
            del kv["stored_selector"]
        try:
            result = self.run(**kv)
            return result
        finally:
            self.workers = None
            time.sleep(1)
            control.automatelist.remove(self)
    def run(self, *l, **kv):
        print "Init"
        self.started = True
        if "name" in kv: del kv["name"] # Ugly work around so it work on the command line.
        debug("Automation %s get thoses parameters : %s %s"%(self.replyname, l, kv))
        self.s_init(*l, **kv)
        print "Init Done"
        while self.workers:
            if self.quit and self.quit != "notified":
                for w in self.workers.keys()[:]:
                    w.throw(Exception("We have to quit !"))
                    self.quit = "notified"
            self.receive(block=False)
        print "Ending our work !"
        if self.quit: raise Exception("Someone asked us to quit !")


        
class autotest(AutomationProcessor):
    def s_init(self, selector, dest):
        self.dest = dest
        self.add_worker(self.bato())
    def bato(self):
        dst = self.dest
        for a in range(3):
            query = self.build_query(funcname="info", target="host", param = { })
            result = (yield Request(query = query, target=dst))
            if not result: raise Exception, "No valid result"
            time.sleep(3)
            self.status = a


class AutomationAPI(AutomationProcessor):
    def s_init(self, *l, **kv):
        """ This method is automaticaly called after automtion initialisation.
        You can use it to handle various parameters passed to your automate and create apropriate worker.
        Note that worker can also create worker if needed.

        """
        # self.add_worker(self.pouet())
        pass




class vmmigrate(AutomationProcessor):
    def s_init(self, selector, destination="*", rate=50):
        self.selector = selector
        self.destination = self.selector_from_pattern(destination, default="host")
        self.rate = int(rate)*1000
        self.add_worker(self.s_send_orders())
        print "#"*80
        print self.selector
        print self.destination
        
    def s_send_orders(self):
        # Liste des vms qu'on doit migrer
        request = Request(query=self.build_query(funcname="stats", target="vm"), selector=self.selector)
        result = (yield request)
        # On veut une reponse pour chaque hote !
        for i in range(1, len(request.sended)):
            result += yield
        self.tomigrate = [ vm for vm in result if vm["name"].lower() != "domain-0"]
        self.status = "VM to migrate : %s"%str([ vm["name"] for vm in self.tomigrate ])
        # For each vm in tomigrate, we'll find a suitable host.
        vmlist = self.tomigrate[:]
        sourcehost = set([ a["host"] for a in vmlist ])
        while vmlist:
            request = Request(query=self.build_query(funcname="info", target="host"), selector=self.destination)
            result = (yield request)
            # On veut une reponse pour chaque hote !
            for i in range(1, len(request.sended)):
                result += yield
            # FIXME : ici on pourrait avoir un algo plus balaise !
            # Pour chaque VMs qu'on a en stock, on cherche le premier hote capable de l'acceuillir
            suitablehost = [ a for a in result if not a["busy"] and a["allocatable"] and a["name"] not in sourcehost]
            suitablehost.sort(key=lambda a: a["nr_vm"])
            #suitablehost.reverse() # On favorise les hotes deja charges !
            for host in suitablehost:
                vm = vmlist[0]
                if (host["cpu_usage"] + int(vm["vcpu"]) < len(host["cpus"])+10 and
                    host["memfreeMB"] > int(vm["memMB"]) and host["storage_freeGB"]*1024*1024*1024 > sum([ vbd["size"] for vbd in vm["vbds"]])*1.05):
                    vmlist.pop(0)
                    self.add_worker(self.s_migrate(vm, host))
                    if not vmlist: break
            if vmlist: time.sleep(2)
        
    def s_migrate(self, vm, target):
        debug("Migrating vm %s to host %s"%(vm["name"], target["name"]))
        #request = Request(query = self.build_query(funcname="install", param=vm), target=host)
        #reply = (yield request)

        src = "#xenhost#%s"%vm["host"]
        dst = "#xenhost#%s"%target["name"]
        lvlist = [ (vbd["realpath"], vbd["size"]) for vbd in vm["vbds"] ]
        vmselector = { "vm": vm["name"] }
        
        # On s'assure que l'hote peut nous acceuillir
        tmp = { "cpu": int(vm["vcpu"]), "memory": vm["memMB"], "disk": int(sum([ vbd["size"] for vbd in vm["vbds"]])/1024/1024/1024)+1 }
        query = self.build_query(funcname="can_host", target="host", param=tmp)
        result = (yield Request(query = query, target=dst))
        if not result:
            print "Aye, we cannot host this VM ..."
            return
        query = self.build_query(funcname="info", target="host")
        result = (yield Request(query=query, target=src))
        if not result: return
        source = result.message[0]

        try:
            query = self.build_query(funcname="set_busy", target="host", param = { "value": "migrating vm %s from %s"%(vm["name"], src) })
            result = (yield Request(query = query, target=dst))
            if not result: raise Exception, "No valid result after setting %s busy"%dst

            query = self.build_query(funcname="set_busy", target="host", param = { "value": "migrating vm %s to %s"%(vm["name"], dst) })
            result = (yield Request(query = query, target=src))
            if not result: raise Exception, "No valid result after setting %s busy"%src

            query = self.build_query(funcname = "create_vmconfig", target="host", param = { "config": vm["config"]["plaintext"], "confpath": vm["config"]["confpath"] })
            result = (yield Request(query = query, target=dst))
            if not result: raise Exception, "No valid result after creating config file for vm %s on %s"%(vm["name"], dst)

            query = self.build_query(funcname = "drbd_create_target", target="vm", param = { "lvlist": lvlist })
            result = (yield Request(query = query, target=dst, selector=vmselector))
            if not result: raise Exception, "Invalid result for create_target on %s"%dst
            portmap_for_src = result.message[0]

            query = self.build_query(funcname = "drbd_swap_to", target="vm")
            result = (yield Request(query = query, target=src, selector=vmselector))
            if not result: raise Exception, "Invalid result after calling drbd_swap_to on %s"%src
            portmap_for_dst = result.message[0]
            
            query = self.build_query(funcname = "drbd_connect", target="vm", param = { "remote": source["ip"], "portmap": portmap_for_dst, "rate": self.rate })
            result = (yield Request(query = query, target=dst, selector=vmselector))
            if not result: raise Exception, "Invalid result for drbd_connect on %s for %s"%(dst, vm["name"])
            
            query = self.build_query(funcname = "drbd_connect", target="vm", param = { "remote": target["ip"], "portmap": portmap_for_src, "rate": self.rate })
            result = (yield Request(query = query, target=src, selector=vmselector))
            if not result: raise Exception, "Invalid result for drbd_connect on %s for %s"%(src, vm["name"])

            print "Waiting for disks sync"
            
            for h in (src, dst):
                query = self.build_query(funcname = "drbd_wait_sync", target="vm")
                result = (yield Request(query = query, target=h, selector=vmselector))
                if not result: raise Exception, "Invalid result for drbd_wait_sync on %s"%h

            print "OK, DATA SYNCED, READY TO CONTINUE"

            query = self.build_query(funcname = "drbd_primary", target="vm")
            result = (yield Request(query = query, target=dst, selector=vmselector))
            if not result: raise Exception, "Invalid result for drbd_primary on %s"%dst

            query = self.build_query(funcname = "migrate", target="vm", param={ "remote": target["ip"] })
            result = (yield Request(query = query, target=src, selector=vmselector))
            if not result: raise Exception, "Invalid result for migrate of %s on %s"%(vm["name"], src)
        except Exception, e:
            print "Error while migrating vm %s (from %s to %s) : %s"%(vm["name"], src, dst, e)
            print "Rolling back"
            time.sleep(5)
            self.add_worker(self.s_rollback(vm, source, target))
            return
        
        print "VM Migrated, cleaning up"
        time.sleep(10) # on attends un peu juste histoire de.
        # FIXME : p'tete faire un truc qui attends que la vm soit running ?

        for h in (src, dst):
            query = self.build_query(funcname = "drbd_swap_from", target="vm")
            result = (yield Request(query = query, target=h, selector=vmselector))
            if not result: print "Failed to restore DRDB status for %s"%h

        query = self.build_query(funcname="delete", target="vm")
        result = (yield Request(query = query, target = src, selector = vmselector))
        if not result: print "Failed to delete vm on old host"

        result = (yield Request(query = self.build_query(funcname="set_busy", target="host", param = { "value": False }),
                                target=dst))
        result = (yield Request(query = self.build_query(funcname="set_busy", target="host", param = { "value": False }),
                                target=src))

        print "VM %s succefully migrated from %s to %s"%(vm["name"], src, dst)
        self.tomigrate.remove(vm)

    def s_rollback(self, vm, source, target):
        src = "#xenhost#%s"%vm["host"]
        dst = "#xenhost#%s"%target["name"]
        lvlist = [ (vbd["realpath"], vbd["size"]) for vbd in vm["vbds"] ]
        vmselector = { "vm": vm["name"] }

        result = (yield Request(query = self.build_query(funcname="set_busy", target="host", param = { "value": False }),
                                target=dst))
        result = (yield Request(query = self.build_query(funcname="set_busy", target="host", param = { "value": False }),
                                target=src))
        yield Request(query = self.build_query(funcname="drbd_swap_from", target="vm"),
                      target = src, selector = vmselector)
        yield Request(query = self.build_query(funcname="drbd_swap_from", target="vm"),
                      target = dst, selector = vmselector)
        yield Request(query = self.build_query(funcname="delete", target="vm"),
                      target = dst, selector = vmselector)
        self.tomigrate.remove(vm) # FIXME

    def s_finish(self): # FIXME : Todo : En faire quelque chose !
        if self.tomigrate:
            print "Those VMs (%s) have NOT been installed : "%len(self.tomigrate), ", ".join([ a["name"] for a in self.tomigrate ])
        else:
            print "Ok"


###########################################################################################
# Les types de commande par defaut.
###########################################################################################
class DefaultCmd(CmdLine, SingleRequestProcessor):
    target = ""

class DefaultHostCmd(DefaultCmd):
    target = "Host"
    defaultpattern = ""
            
class DefaultDangerousHostCmd(DefaultHostCmd):
    defaultpattern = "__NOMATCH__"

class DefaultVmCmd(DefaultCmd):
    target = "VM"
    defaultpattern = ""
    
class DefaultDangerousVmCmd(DefaultVmCmd):
    defaultpattern = "__NOMATCH__"

class InternalCmd(DefaultCmd):
    target = "Internal"
    defaultpattern = ""

class InternalDangerousCmd(InternalCmd):
    defaultpattern = "__NOMATCH__"

class AutomationCmd(DefaultCmd):
    target = "Automation"
    defaultpattern = "__NOMATCH__"
    helptext = textwrap.dedent("""\
    Allow you to migrate vm from an host to an other.
    Eg, the following commandline will migrate every vm on xen-1 to xen-2 :
      %prog vmmigrate -d xen-2 xen-1/*

    You can background the action with the -b flags ; you can then follow it with %prog tasks 
    """)
    options = [ atot( "-b", "--background", help="Spawn automation on cluster, detached.", default=False, action="store_true"),
                atot( "-n", "--name", help="Unique name for automation. Generated if not specified", default="a-%s"%random.randint(1,1000000)) ]
    def selector_from_pattern(self, pattern, default=None):
        return super(CmdLine, self).selector_from_pattern(pattern, default="vm")
    def parse_options(self, *l, **kv):
        # Quand on lance une automation, on veut qu'elle s'execute partout
        # et on veut passer le selector en parametre afin que l'execution de l'automation
        # puisse se faire sur ce qu'on a selectionner
        selector, options = super(AutomationCmd, self).parse_options(*l, **kv)
        if "background" in options:
            bg = options["background"]
            del options["background"] # Strip the option from the automation : it's used to control how it's spawn
        else: bg = False
        if not bg:
            self.automate = Automations[self.funcname](name=options.get("name", "NoName"))
            # We override just run. All the handle_reply & co are conserved from the original CMD processor
            # so we wont have any hack & co for redudancy
            self.run = self.run_debug
        else:
            options["stored_selector"] = selector
            selector = self.selector_from_pattern("*/") # Every host execute the automation
        return (selector, options)
    def run_debug(self, *l, **kv):
        self.automate.run(*l, **kv)
    
###########################################################################################


class Update(InternalCmd):
    helptext = "Update the cluster with the current binary if it's version number is more recent."
    def build_query(self, **kv):
        """ Build the query by looking for the keyword target, pattern, funcname, param """
        tmp = {}
        tmp["version"] = VERSION
        tmp["code"] = file(globals()["__file__"]).read()
        target, funcname, param = super(Update, self).build_query(**kv)
        return (target, funcname, tmp)

class Version(InternalCmd): pass

class Tasks(InternalCmd):
    helptext = "See the tasks executed on matched host."
    def format_replies(self, reply, options):
        result = []
        for r in  reply["result"]:
            s = StatusTracker.deserialize(r)
            result.append(s.name)
            result.append("    " + "\n    ".join(s.getlog()))
        return "\n".join(result)

class StopAutomation(InternalCmd):
    helptext = "Allow you to try to kill an automation by it's name."
    funcname = "stop_automation"
    options = [ atot( "-n", "--name", help="identifier", default="zoobr"),]

class VmDrbd_swap_to(DefaultDangerousVmCmd): pass

class VmDrbd_swap_from(DefaultDangerousVmCmd): pass


class VmList(DefaultVmCmd):
    helptext = textwrap.dedent("""\
    List all vm matched by the selector pattern.
    Eg :
      List all vm on the cluster :
        %prog vmlist
      Sort it by vm name :
        %prog vmlist -s name
      List all vm on one host :
        %prog vmlist xen-1/*
      List all vm starting with stream- :
        %prog vmlist stream-*
    """)
    funcname = "info"
    fmt = "%(host)10s  %(name)20s  %(vcpu)3s  %(memMB)4s  %(power_state)15s"
    sortkeys = ( "host", "name")
    def end_replies(self, total, options):
        result = []
        result.append("%(reply_count)d vms :"%total)
        result.append("\t- %d Dom0 (Running)"%total.get("dom0", 0))
        result.append("\t- %d Running"%(total.get("Running", 0) - total.get("dom0", 0)))
        result.append("\t- %d Halted"%total.get("Halted", 0))
        result.append("\t- %d Paused"%total.get("Paused", 0))
        return "\n".join(result)


class VmStats(DefaultVmCmd):
    helptext = textwrap.dedent("""\
    Get the xen statistics for vm matched by selector pattern.
    See help on vmlist for more details""")
    fmt = "%(host)6s  %(name)20s  %(vcpu)4s  %(memMB)5s  %(power_state)11s %(cpu_usage)9s %% %(disk_usage)14s %(net_usage)14s  %(disk_used)20s"
    sortkeys = ("host", "name")
    def reply_headers(self, replies, options):
        if len(replies) > 0:
            z = replies[0].keys()
            z.append("cpu_usage")
            z.append("disk_usage")
            z.append("net_usage")
            z.append("disk_used")
            head = dict(zip(z, z))
            return self.fmt%head
    def format_replies(self, vm, options):
        vm["cpu_usage"] =  "% 4.1f"%(sum(vm.get("vcpu_usage", {None:0}).values())*100)
        for display, key in ( ("disk_usage", "vbds"), ("net_usage", "vifs") ):
            if key not in vm:
                vm[display] = "None"
                continue
            tmp = [ (v.get("io_read_kbs",0), v.get("io_write_kbs",0)) for v in vm[key] ]
            read = sum([ i[0] for i in tmp ])/1024
            write = sum([ i[1] for i in tmp ])/1024
            vm[display] = "%5.0f/%5.0f KB"%(read, write)
        tmp = []
        for vbd in vm.get("vbds", []):
            if "free" in vbd and "size" in vbd:
                tmp.append("%s %4.1f%%"%(vbd["name"], (1-vbd["free"]/float(vbd["size"]))*100))
        vm["disk_used"] = ",".join(tmp)
        return self.fmt%vm

class HostAllocatable(DefaultHostCmd):
    helptext = "Set the host to allocatable state. Usefull after automation crash :)"
    options = [ atot("--false", help="Set to not allocatable", dest="value", default=True, action="store_false") ]

class HostBusy(DefaultHostCmd):
    helptext = "Set the busy flag on matched host"
    options = [ atot("--true", help="Set busy", dest="value", default=False, action="store_true") ]
    funcname = "set_busy"

class HostMdStat(DefaultHostCmd):
    helptext = "Return the md stat of each matched host"
    
class HostDate(DefaultHostCmd):
    helptext = "Display the system date of each matched host"

class HostRestartNtp(DefaultHostCmd):
    funcname = "ntp_restart"
    helptext = "Restart the ntpd process on the matched host"

class XendRestart(DefaultHostCmd):
    funcname = "xend_restart"
    helptext = "Restart the xend process on the matched host [ DANGEROUS ]"

class HostExecute(DefaultHostCmd):
    options = [ atot("-c", "--cmd", help="cmd to execute", dest="cmd"),
                atot("-t", "--timeout", help="timeout", type=int, default=5 )]
    helptext = "Execute an arbitrary command on matched host [ DANGEROUS ]"

class HostLvList(DefaultHostCmd):
    funcname = "lv_list"
    fmt = "%(host)6s %(name)25s %(size)10s"
    sortkeys = ("host", "name")

class HostList(DefaultHostCmd):
    helptext = "List matched host with some useful details"
    funcname = "info"
    fmt = "%(name)6s  %(hvm)5s  %(enabled)8s %(allocatable)10s %(cpu)4s  %(memMB)5s %(memfreeMB)10s  %(nr_vm)6s %(storage_freeGB)15s %(xen_version)8s %(kernel_version)20s %(cpu_model)s"
    sortkeys = ("name",)
    def reply_headers(self, replies, options):
        if len(replies)> 0:
            z = replies[0].keys()
            z.append("cpu_model")
            head = dict(zip(z, z))
            return self.fmt%head
    def format_replies(self, host, options):
        host["cpu_model"] = host["cpus"][0]["cpuname"]
        return self.fmt%host
    def end_replies(self, total, options):
        result = []
        result.append("%d host responded (%d enabled, %d busy) for a total of %d corethread hosting %d vm"%
                      (total["reply_count"], total["enabled"], total["busy"], total["cpu"], total["nr_vm"]))
        result.append("Memory : %s free on %s ; Used at %.2f %%"%(megabyze(total["memfree"]), megabyze(total["mem"]),
                                                                  (total["mem"] - total["memfree"])/float(total["mem"])*100))
        result.append("Disks  : %s free on %s ; Used at %.2f %%"%(megabyze(total["storage_free"]), megabyze(total["storage_size"]),
                                                                  (total["storage_size"] - total["storage_free"])/float(total["storage_size"])*100))
        return "\n".join(result)



class HostStats(DefaultHostCmd):
    helptext = "Gather statistics on matched host"
    fmt = "%(name)6s  %(hvm)5s  %(enabled)8s %(cpu)4s %(cpu_usage)11s %(memMB)5s %(memfreeMB)10s  %(nr_vm)6s %(disk_usage)14s %(net_usage)14s"
    sortkeys = ("name",)
    def reply_headers(self, replies, options):
        if len(replies)> 0:
            z = replies[0].keys()
            z.append("disk_usage")
            z.append("net_usage")
            head = dict(zip(z, z))
            return self.fmt%head
    def format_replies(self, host, options):
        host["cpu_usage"] = "%.1f %%"%(host["cpu_usage"]*100)
        host["disk_usage"] = "%5.0f / %5.0f KB"%(sum([ i["read"] for i in host["disk"].values() ]) / 1024,
                                                 sum([ i["write"] for i in host["disk"].values() ]) / 1024)
        host["net_usage"] = "%5.0f / %5.0f KB"%(sum([ i["receive"] for i in host["interface"].values() ]) / 1024,
                                                sum([ i["transmit"] for i in host["interface"].values() ]) / 1024)
        return self.fmt%host
    def end_replies(self, total, options):
        print total
        result = []
        result.append("%d host responded (%d enabled, %d busy) for a total of %d corethread hosting %d vm"%
                      (total["reply_count"], total["enabled"], total["busy"], total["cpu"], total["nr_vm"]))
        result.append("CPU    : %.1f free on %s ; Used at %.2f %%"%(total["cpu"] - total["cpu_usage"], total["cpu"],
                                                                    100 - (total["cpu"] - total["cpu_usage"])/float(total["cpu"])*100))
        result.append("Memory : %s free on %s ; Used at %.2f %%"%(megabyze(total["memfree"]), megabyze(total["mem"]),
                                                                  (total["mem"] - total["memfree"])/float(total["mem"])*100))
        result.append("Disks  : %s free on %s ; Used at %.2f %%"%(megabyze(total["storage_free"]), megabyze(total["storage_size"]),
                                                                  (total["storage_size"] - total["storage_free"])/float(total["storage_size"])*100))
        return "\n".join(result)


class ConfigList(DefaultVmCmd):
    helptext = "List vm config file on host."
    fmt = "%(name)s"
    funcname = "read_config"

class CanHost(DefaultHostCmd):
    helptext = textwrap.dedent("""\
    Return the list of host able to alloc specified ressource.
    Eg : list host able to allocate 100 GB of disk space and 2G of memory :
      %prog canhost -m 2000 -d 100
    """)
    funcname = "can_host"
    fmt = "%s"
    options = [ atot("-c", "--cpu", help="Number of cpu", default= 0, type="int"),
                atot("-m", "--memory", help="Memory size in Megabytes", default=1000, type="int"),
                atot("-d", "--disk", help="Disk size in gigabyte", default=10, type="int"),
                atot("--hvm", help="Disk size in gigabyte", default=False, action="store_true") ]
    def reply_headers(self, replies, options):
        result = []
        result.append("The following host are eligible to host a vm with those parameters :")
        result.append("  Cpus: %(cpu)s   Memory: %(memory)s Mo   Disks: %(disk)s GB   HVM: %(hvm)s"%options)
        return "\n".join(result)
    
class VmStop(DefaultDangerousVmCmd):
    helptext = "Stop all matched vm(s)"
    options = [ atot("-f", "--forced", default=False, action="store_true") ]
    
class VmStart(DefaultDangerousVmCmd):
    helptext = "Start all matched vm(s)"
    
class VmRestart(DefaultDangerousVmCmd):
    helptext = textwrap.dedent("""\
    Restart all matched vm(s)
    Note that the vm(s) will be stopped uncleanly an started after a short delay.
    """)
    options = [ atot("-f", "--forced", default=False, action="store_true") ]
    
class VmPause(DefaultDangerousVmCmd):
    helptext = "Pause the matched vm(s)"
class VmUnPause(DefaultDangerousVmCmd):
    helptext = "Unpause the matched vm(s)"
class VmDelete(DefaultDangerousVmCmd):
    helptext = textwrap.dedent("""\
    Delete PERMANENTLY all the matched vm(s) [DANGEROUS]
    This command stop the vm, delete the corresponding disk and the config file.
    """)
    

class VmSetMemory(DefaultDangerousVmCmd):
    helptext = "Set memory for the matched vm(s)"
    funcname = "set_memory"
    options = [ atot("-m", "--memory", help="Memory size in Megabytes", type="int"), ]
class VmSetCpu(DefaultDangerousVmCmd):
    helptext = "Set the number of cpu for the matched vm(s)"
    funcname = "set_cpu"
    options = [ atot("-c", "--cpu", help="Number of cpu", type="int"), ]

class VmDeploy(DefaultHostCmd):
    """
    Blan deployment system.

    This class allow to launch a bunch of new vms in one command, guessing
    what is the next free available hostname.
    It should be rewritten as an automation.
    """
    options = [ atot("-t", "--template", help="Template name."),
                atot("-n", "--number", help="Number of VM to create", default=1, type="int"),
                atot("-i", "--index", help="Starting number for automatic numerotation", default=1, type="int"),
                atot("--hostname", help="string to use to generate hostname", default="%(type)s-%(i).3d"),
                atot("--dummy", help="dummy mode : do nothing", default=False, action="store_true") ]
    templates = { "flashstream": { "cpu": 2, "memory": 1000, "disk": 10 },
                  "oldstream": { "cpu": 2, "memory": 2000, "disk": 10 },
                  "oldstream-timeshift": { "cpu": 2, "memory": 1000, "disk": 10 },
                  }
    def installvm(self):
        host = self.availablehost.pop(0)
        vm = self.toinstall.pop(0)
        debug("Installing vm %s on host %s"%(vm["hostname"], host))
        request = Request(query = self.build_query(funcname="install", param=vm), target=host)
        reply = (yield request)
        if reply and reply.message[0]["result"] == CODE_OK:
            debug("Vm %s on host %s succefully installed"%(vm["hostname"], host))
            request = Request(query = self.build_query(funcname="can_host", param=vm["args"]), target = host)
            if (yield request):
                self.availablehost.append(host)
        else:
            debug("host %s failed to install vm %s, retrying on an other host"%(host, vm["hostname"]))
            self.toinstall.append(vm)
    def generate_vms_config(self, options):
        installtype = options.get("template", None)
        # On liste les vms actuelles de ce type pour determiner les noms des vms a installer
        request = Request(query=self.build_query(funcname="info", target="vm"),
                          selector={ "vm": "%s-*"%installtype })
        replies = self.collect_replies(request)
        indexes = []
        currentnames = set([ int(re.match(".*-([0-9]+).*", i["name"]).group(1)) for i in replies ])
        i = options.get("index", 1)
        while len(indexes) < options["number"] and i <= 253:
            if i not in currentnames : indexes.append(i)
            i += 1
        if len(indexes) < options["number"]:
            exit("We can only create %d Vms, requested %s"%(len(indexes), options["number"]))
        hostnames = [ options["hostname"]%({"type": installtype, "i": i}) for i in indexes ]
        print "Generating vms : %s"%hostnames
        # On genere les parametres de configurations
        tmp = self.templates[installtype].copy()
        if options.get("dummy", False): tmp["dummy"] = True
        result = []
        for hostname in hostnames:
            result.append({ "templatename": installtype,
                            "hostname": hostname,
                            "args": tmp })
        return result
    def run(self):
        selector, options = self.parse_options()
        # Init ...
        self.toinstall = []
        self.availablehost = []
        # On prends en entre un type de VM et un nombre
        installtype = options.get("template", None)
        if not installtype or installtype not in self.templates:
            exit("You must specify a valid template (%s)"%self.templates.keys())
        self.toinstall = self.generate_vms_config(options)
        # On selectionne les hotes capables d'installer ce type de machine
        request = Request(query = self.build_query(funcname="can_install", param= { "templatename" : installtype }), selector = selector)
        replies = self.collect_replies(request)
        can_install = set([ "#xenhost#%s"%i["identifier"] for i in replies if i])
        # On install sur les machines capable tant qu'elle reponde oui a can_host
        request = Request(self.build_query(funcname="can_host", param=self.templates[installtype]), selector = selector)
        replies = self.collect_replies(request)
        can_host = set([ "#xenhost#%s"%i["identifier"] for i in replies if i])
        self.availablehost = list(can_install.intersection(can_host))
        print "Suitable host for install : ", self.availablehost
        while (self.toinstall and self.availablehost) or self.workers:
            while self.availablehost and self.toinstall:
                self.add_worker(self.installvm())
            self.run_worker()
        if self.toinstall:
            print "Those VMs (%s) have NOT been installed : "%len(self.toinstall), ", ".join([ a["hostname"] for a in self.toinstall ])
        else: print "Finished !"


class VmMigrate(AutomationCmd):
    helptext = textwrap.dedent("""\
    Migrate the matched vms on selected host.
    This command allow the live-migration, with disk, of a matched vm(s) to compatible host.
    It use drbd to syncronise your disk, switching from and to the appropriate backend device
    transparently.
    Eg :
      Migrate all vm from xen-2 to xen-8 :
        %prog vmmigrate -d xen-8 xen-2/*
      Migrate all vm from xen-2 to the cloud :
        %prog vmmigrate xen-2/*
    """)
    options = AutomationCmd.options + [
        atot("-d", "--destination", help="Destination Host Pattern", default="*"),
        atot("-r", "--rate", help="speed of each drbd in Megabyte/s", default=50, type="int"),
        ]
 
    
class AutoTest(AutomationCmd):
    options = AutomationCmd.options + [
        atot( "-d", "--dest", help="param string", default="#xenhost#xen-1"),
        ] #default="a-%s"%random.randint(1,1000000)

def daemon():
    process = XenProcessor()
    process.serve()

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print CmdLine().print_help_and_exit()
    elif sys.argv[1] == "-d":
        daemon()
    else:
        if sys.argv[1] in Cmds:
            print Cmds[sys.argv[1]]().parse_and_run()
        else:
            print CmdLine().print_help_and_exit()
