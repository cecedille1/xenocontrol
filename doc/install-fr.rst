Prise en main rapide de XenoControl
===================================

Principe de fonctionnement
--------------------------

XenoControl est un script Python permettant de gérer un ensemble de
serveurs Xen de manière unifiée et décentralisée. Cela signifie qu'il
est possible de lancer des commandes depuis n'importe lequel des
serveurs gérés par XenoControl, et que ces commandes peuvent agir sur
n'importe quel autre serveur (ou sur un ensemble de serveurs) gérés
par XenoControl. Il n'y a pas de console centralisée ou de nœud
principal qui constituerait un SPOF.

Le script se divise en une partie serveur (tournant en tâche de fond
sur chaque hôte Xen) et une partie cliente, accessible en ligne de
commande. La communication entre les clients et les serveurs se fait
grâce au bus de communication spread. Afin de faciliter le déploiement
et la mise à jour, le client et le serveur sont intégrés dans le même
fichier Python (.py).

Le bus spread permet à un ensemble de machines de communiquer
efficacement en multicast. Il assure la fiabilité des transmissions
(le code n'a pas besoin de se préoccuper des problématiques d'accès
concurrents, de perte de paquets, etc). Chaque démon « serveur » de
XenoControl se connecte au bus spread et attend ensuite de recevoir
les requêtes venant des clients.

Le principe de communication est assez simple. Lorsqu'on lance une
commande à partir d'un client, la requête correspondante est toujours
envoyée à tous les serveurs. Chaque commande agit sur un sélecteur
(par exemple : stream*.tvr pour indiquer « toutes les machines
virtuelles dont le nom correspond à stream*.tvr »). Tous les serveurs
reçoivent la requête, examinent le sélecteur, et chaque serveur qui
dispose d'une ressource correspondant au sélecteur répondra en accord.

Installation
------------

Prérequis
^^^^^^^^^

Le serveur XenoControl nécessite :

* python 2.4
* python-spread
* screen (pour lancer XenoControl en mode démon)
* un démon spread

Si on souhaite utiliser XenoControl pour réaliser des live migrations
de machines virtuelles s'exécutant sur du stockage local (LVM ou
autre), il est nécessaire de disposer du module drbd. Il ne faut pas
charger le module drbd manuellement : c'est XenoControl qui doit
s'occuper de son chargement.

En effet, le module drbd 8.3 invoque automatiquement un helper script
lorsqu'il désactive une ressource. Le helper par défaut tente de lire
un fichier de configuration. Par défaut, ce fichier de configuration
n'existe pas (XenoControl n'en fait pas usage) : il faut donc
spécifier, lors du chargement du module, que le helper à utiliser est
/bin/true afin d'éviter des effets de bords indésirables.

Configuration de spread
^^^^^^^^^^^^^^^^^^^^^^^

L'installation et la configuration de spread sont difficiles à automatiser 
complètement.

La première chose à faire est d'installer les paquetages correspondants ::

    # apt-get install spread python-spread screen

    # vi /etc/default/spread
    #### Mettre ENABLED a 1

Ensuite, il faut créer le fichier de configuration de spread, 
/etc/spread/spread.conf ::

    Spread_Segment  10.16.0.255:4803 {
            xen-1        10.16.0.1
            xen-2        10.16.0.2

    }

Il faut déclarer un Spread_Segment associé à l'adresse de broadcast du
réseau local sur lequel se trouvent les hôtes Xen (sauf si l'on a un
réseau multicast). Dans ce segment, il faut renseigner toutes les
adresses des hôtes que l'on veut gérer avec XenoControl (ou à partir
desquels on souhaite lancer des commandes).

Pour éviter de modifier la configuration de tous les hôtes à chaque
fois qu'on ajoute un serveur, il est possible de préconfigurer à
l'avance un ensemble d'hôtes, même si certains n'existent pas encore
(le fait qu'il manque des hôtes par rapport à ce qui est indiqué dans
le fichier de configuration n'empêchera pas spread de fonctionner
correctement).

Note : le nombre maximal d'hôtes par segment est de 100.

Attention : spread est particulièrement capricieux au niveau de la
résolution des noms des hôtes ; il est donc nécéssaire d'avoir dans
/etc/hosts une entrée pour l'hôte local établissant une correspondance
identique à celle utilisée dans le fichier de configuration de
spread. Il n'est pas nécessaire de préciser tous les hôtes - le local
suffit ::

    echo "10.16.0.1    xen-1" >> /etc/hosts

On peut ensuite lancer le demon spread ::

    /etc/init.d/spread start

Pour vérifier le bon fonctionnement de spread, on peut utiliser la
commande « spuser ». Son affichage doit ressembler à celui-ci ::

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

Si cela ne fonctionne pas, il faut reprendre les étapes une par
une. Note : spread est plutôt avare de messages d'erreur !  Ne pas
hésiter à utiliser les outils habituels (strace, tcpdump...) si
nécessaire.  

Lancement du démon 
^^^^^^^^^^^^^^^^^^^

Par défaut, lorsqu'on lance xenocontrol.py, il se comporte comme
client. Il devient serveur si on ajoute l'option « -d ».

XenoControl n'est pas capable de se démoniser tout seul (pour
l'instant) ; mais on peut facilement utiliser (par exemple) screen
pour cela.

Exemple d'initscript permettant le lancement automatique du serveur
XenoControl au démarrage du système ::

    # cat etc/init.d/xenocontrol
    cd /root
    screen -m -d ./xenocontrol.py -d

Utilisation du client
---------------------

Lancer le script sans argument affiche l'aide.

Les commandes les plus courantes sont :

* vmlist - liste les VMs du cluster ;
* hostlist - liste les hôtes ;
* vmstats - donne des statistiques détaillées sur les VMs ;
* hoststats - donne des statistiques sur les hôtes.

Les autres sont à découvrir en parcourant l'aide ::

    ./xenocontrol.py hostlist
    Got a reply from #xenhost#xen-1
    Got a reply from #xenhost#xen-2
      name    hvm   enabled allocatable  cpu  memMB  memfreeMB   nr_vm  storage_freeGB xen_version       kernel_version cpu_model
     xen-1  False      True       True    4   4095          2       5             233      3.2   2.6.26-2-xen-amd64 Intel(R) Xeon(TM) CPU 3.20GHz
     xen-2  False      True       True    4   9983        128       1             857      3.2   2.6.26-2-xen-amd64 Dual Core AMD Opteron(tm) Processor 265
    2 host responded (2 enabled, 0 busy) for a total of 8 corethread hosting 6 vm
    Memory : 130.6M free on 13.7G ; Used at 99.07 %
    Disks  : 1090.3G free on 1130.5G ; Used at 3.55 %
