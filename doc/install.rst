XenoControl installation howto
==============================

How ir works
------------

XenoControl is a Python script allowing to manage a set of Xen servers
from a decentralized console. You can run any command from any of
the hosts managed by XenoControl; and those commands can interact
with any other server (or set of servers) managed by XenoControl.
There is no centralized console or management node (which would
be a SPOF).

The script is split between a server component (running in background
on each Xen host) and a client component, designed for command-line
use. Communication between clients and servers relies on the spread
communication bus. To make deployment and upgrades easier, client and
server are built into the same Python file (.py).

The spread bus allows a set of hosts to communicate efficiently
using multicast. It ensures the reliability of message exchanges
(our code does not need to bother with concurrent access issues,
packet losses, etc). Each XenoControl daemon connects to the spread
bus (using the local spread broker) and then waits for client requests.

The communication protocol is quite simple. When a command is issued
by a client, the corresponding request is sent to all servers. Each
command specifies a selector (for instance, 'stream*.tvr' to indicate
'every virtual machine whose name is matched by 'stream*.tvr'). All
Xen hosts receive the request, parse the selector, and each server
hosting a relevant ressource will reply to the original requestor.

Installation
------------

Requirements
^^^^^^^^^^^^

XenoControl requires:

* python 2.4
* python-spread
* screen (to start XenoControl in daemon mode)
* a running spread daemon

If you want to use XenoControl to do live migration of virtual machines
using local storage (LVM or similar), you will also need the drbd module.
Do not load the drbd module: XenoControl must load it itself.

Warning, by default, drbd 8.3 will automatically run a helper script
when disabling a ressource. The helper tries to run a configuration file.
By default, this configuration file does not exist (XenoControl does not
use it anyway); therefore, when loading the drbd module, you have to
set the helper script to '/bin/true' to avoid any unwanted side effect.

Setup spread
^^^^^^^^^^^^

It is difficult to automatically install and setup spread; therefore,
you have to complete a few manual steps.

First, you have to install the needed packages::

    # apt-get install spread python-spread screen

    # vi /etc/default/spread
    #### Mettre ENABLED a 1

Next, create the spread configuration file, /etc/spread/spread.conf ::

    Spread_Segment  10.16.0.255:4803 {
            xen-1        10.16.0.1
            xen-2        10.16.0.2

    }

You have to declare a Spread_Segment corresponding to the broadacst
address of the LAN used by the Xen hosts (unless you already have
setup a site-wide multicast network). In this segment, you have
to fill in the addresses of *all* hosts to be managed by XenoControl
(as well as those from which you want to run XenoControl commands).

You will probably add more Xen hosts later. To avoid reconfiguring
spread on each member of your cluster each time you add an host
to the cluster, you can declare hosts in the configuration file even
if they do not exist yet. 

However, note that the maximal number of hosts is 100.

Warning: spread can be quite picky regarding DNS resolution. To
avoid problems, you should add the local host in /etc/hosts, using
the same name and IP address as the one specified in the spread
configuration. You don't need to declare every host; the local host
is enough::

    echo "10.16.0.1    xen-1" >> /etc/hosts

Then, go ahead and start spread::

    /etc/init.d/spread start

To check that spread is working properly, you can use the 'spuser' 
command. It should display something like this::

    # spuser
    Spread library version is 3.17.4
    User: connected to 4803@localhost with private group #user#xen-1

    ==========
    User Menu:
    ----------

            j <group> -- join a group
            l <group> -- leave a group

            s <group> -- send a message
            b <group> -- send a burst of messages

            r -- receive a message (stuck)
            p -- poll for a message
            e -- enable asynchonous read (default)
            d -- disable asynchronous read

            q -- quit

    User>

If spread does not work, doublecheck everything from the beginning.
Note: spread is not very verbose and does not print a lot of
insightful error messages! Don't hesitate to use the usual tools
if necessary (strace, tcpdump, etc).

Starting XenoControl
^^^^^^^^^^^^^^^^^^^^

If you run xenocontrol.py, it will behave as a client. It will
act as a server if you add option '-d'.

XenoControl is not able (yet) to become a daemon by itself;
but you can use (e.g.) screen for that.

Here is a minimal initscript to start automatically XenoControl
at system startup::

    # cat etc/init.d/xenocontrol
    cd /root
    screen -m -d ./xenocontrol.py -d

Using the client
----------------

Running the script with no argument will display some help.

The most useful commands are:

* vmlist - list all VMs on the cluster;
* hostlist - list all Xen hosts;
* vmstats - display detailed statistics about the VMs;
* hoststats - display detailed statistics about the hosts.

Check the other commands but reading the help (and the source code)::

    ./xenocontrol.py hostlist
    Got a reply from #xenhost#xen-1
    Got a reply from #xenhost#xen-2
      name    hvm   enabled allocatable  cpu  memMB  memfreeMB   nr_vm  storage_freeGB xen_version       kernel_version cpu_model
     xen-1  False      True       True    4   4095          2       5             233      3.2   2.6.26-2-xen-amd64 Intel(R) Xeon(TM) CPU 3.20GHz
     xen-2  False      True       True    4   9983        128       1             857      3.2   2.6.26-2-xen-amd64 Dual Core AMD Opteron(tm) Processor 265
    2 host responded (2 enabled, 0 busy) for a total of 8 corethread hosting 6 vm
    Memory : 130.6M free on 13.7G ; Used at 99.07 %
    Disks  : 1090.3G free on 1130.5G ; Used at 3.55 %
