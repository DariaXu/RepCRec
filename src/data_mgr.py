from Site import Site, Variable
from const import LockState
import logging
import re

logger = logging.getLogger(__name__)

class DataMgr(object):
    def __init__(self, numOfSites, numOfVariable) -> None:
        """
        Initialize the Data Manger.

        Parameters
        -----------
        numOfSites: int 
            Total number of sites.
        numOfVariable: str 
            Total number of variable.
        """
        self.sites = {}
        self._init_sites(numOfSites, numOfVariable)

    def _init_sites(self, numOfSites, numOfVariable):
        """
        Initialize sites.
        The odd indexed variables are at one site each. Even indexed variables are at all sites. 
        Each variable xi is initialized to the value 10i (10 times i).
        """
        oddVars = [Variable("x"+str(i), 10*i) for i in range(1,numOfVariable+1) if not i%2]

        for site in range(1, numOfSites+1):
            evenVars = [Variable("x"+str(i), 10*i, site) for i in range(1,numOfVariable+1) if not i%2]
            oddVars = [Variable("x"+str(i), 10*i) for i in range(1,numOfVariable+1) if i%2 and (i%10 +1) == site]
            curSite = Site(str(site), evenVars+oddVars)
            curSite.isActive = True
            self.sites[str(site)] = curSite

    def get_site_index(self, x): 
        """
        Get the site index

        Parameters
        -----------
        x: str
            variable name

        Returns: int 
        -----------
        None if x is on multiple sites; site index if x is not replicated 
        """
        siteNum = int(re.split('(\d+)',x)[1])
        # siteNum = int(x[x.find('.')+1:])
        if siteNum % 2:
            return str(siteNum+1)

        return None


    def get_available_sites(self): 
        """
        Return a list of sites that are up.

        Returns: list
        -----------
        List of available sites.
        """

        return [site for site in self.sites.values() if site.isActive]
    
    def get_available_sites_for_variable(self, x): 
        """
        Get all sites that is active and contain x.

        Parameters
        -----------
            x: variable name
        Returns: list
        -----------
        The list of sites that are up and contain x.
        """

        # varNum = int(x[x.find('.')+1:])
        siteIndex = self.get_site_index(x)
        if siteIndex != None:
            # odd variable only on one site
            site=self.sites[siteIndex]
            if site.isActive and site.ifContains(x):
                return [site]
            return []
        
        # even variable, can have multiple copies
        availableSites = self.get_available_sites()
        allSites = [site for site in availableSites if site.ifContains(x)]

        return allSites

    def recover(self, siteNum, tick):
        """
        Recover site

        Parameters
        -----------
        siteNum: int 
            Site number
        tick: int
            current tick
        """
        self.sites[siteNum].recover(tick)
        logger.debug(f"{tick}: Recovered Site {siteNum}")

    def fail(self, siteNum, tick):
        """
        Fail site

        Parameters
        -----------
        siteNum: int 
            Site number
        tick: int
            current tick
        """
        self.sites[siteNum].fail(tick)
        logger.debug(f"{tick}: Failed Site {siteNum}")
    
    def request_read_only(self, transaction, x):
        """
        Request read only operation.

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 

        Returns: bool
        -----------
        The value of variable x if read successfully; None otherwise
        """
        logger.debug(f"{transaction.name} requests read only on variable {x}.")
        sites = self.get_available_sites_for_variable(x)
        if not sites:
            # no available site is up, need to wait if variable is not replicated
            logger.debug(f"{transaction.name} fail to read only on variable {x}! No active sites.")
            return None

        if self.get_site_index(x) != None:
            # odd variable
            return sites[0].read_only(transaction, x)
        
        # Replicated variable
        for site in sites:
            # if site recovered after read only transaction begin, there should be no copies on that site
            # if_available_to_read_only might be redundant 
            if site.if_available_to_read_only(transaction, x):
                # read the first available  
                return site.read_only(transaction, x)

        logger.debug(f"{transaction.name} fail to read only on variable {x}! Can't be read on sites {sites}.")
        return None

    def request_read(self, transaction, x, tick):
        """
        Request read operation.

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
        tick: int
            current tick

        Returns: tuple (bool, list/str)
        -----------
        First element is the boolean showing whether read successfully processed.
        For Second element, if read success, it is the read value;
        if read fail, it is the a list of lock objects blocking this read.
        """

        logger.debug(f"{transaction.name} requests read on variable {x}.")
        sites = self.get_available_sites_for_variable(x)
        if not sites:
            # All sites 
            logger.debug(f"{transaction.name} fail to read on variable {x}! No active sites.")
            return (False, [])

        blocked = []
        for site in sites:
            if self.get_site_index(x) != None or site.if_available_to_read(transaction, x):
                # for not replicated variable, no need the check the commit time(if_available_to_read)
                blocked = site.lock_variable(transaction, x, LockState.R_LOCK, tick)
                if not blocked:
                    return (True, site.read(transaction, x))
                
        logger.debug(f"{transaction.name} fail to read on variable {x}! Can't be read on sites {sites}.")
        return (False, blocked)

    def request_write(self, transaction, x, val, tick):
        """
        Request write operation.

        Parameters:
        -----------
        transaction: Transaction Object
		x: str
            Variable name
        val: str
            The value to write
        tick: int
            Current tick

        Returns: tuple (bool, list)
        -----------
        First element is the boolean showing whether write successfully processed.
        For Second element, if write success, it is a empty list;
        if write fail, it is the a list of lock objects blocking this write.
        """
        logger.debug(f"{transaction.name} requests write on variable {x}: {val}.")
        sites = self.get_available_sites_for_variable(x)
        if not sites:
            logger.debug(f"{transaction.name} fail to write on variable {x}! No active sites.")
            return (False, [])

        blocked = []
        for site in sites:
            blocked += site.get_rw_lock_block(transaction, x)

        if blocked:
            # can't require write lock on all sites
            return (False, blocked)

        for site in sites:
            site.lock_variable(transaction, x, LockState.RW_LOCK, tick)
            site.write(transaction, x, val)

        return (True, [])

    def abort_on_all_sites(self, transaction):
        """
        Request to abort transaction.

        Parameters
        -----------
        transaction: transaction object
        """

        logger.debug(f"Abort {transaction.name} on all sites.")

        sites = self.get_available_sites()
        for site in sites:
            site.abort(transaction)

    def commit_on_all_sites(self, transaction, tick):
        """
        Request to commit transaction.

        Parameters
        -----------
        transaction: transaction object
        tick: int
            Current tick
        """

        logger.debug(f"Commit {transaction.name} on all sites.")

        sites = self.get_available_sites()
        for site in sites:
            site.commit(transaction, tick)


    def dump_all_sites(self):
        """ dump """
        for name, site in self.sites.items():
            variables = list(site.committedVariables.keys())
            num = sorted([int(v[1:]) for v in variables])
            sortedVars = ['x'+ str(n) for n in num]

            strVars = f"Site {name} - "
            for v in sortedVars:
                strVars += str(site.committedVariables[v]) + ", "

            logger.info(strVars[:-2])
