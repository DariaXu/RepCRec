from Site import Site
from const import LockState
import logging
import re

logger = logging.getLogger(__name__)

class Variable:
    def __init__(self, name, value, onSite=None) -> None:
        self.name = name
        self.value = value
        self.lastCommittedTime = -1
        # if this variable is not a copy, onSite will be None; 
        # otherwise, onSite will be the site number where this copy is on 
        self.onSite = onSite

    def __str__(self) -> str:
        return f"{self.name}: {self.value}"

class DataMgr(object):
    def __init__(self, numOfSites, numOfVariable) -> None:
        self.sites = {}
        self.init_sites(numOfSites, numOfVariable)

    def init_sites(self, numOfSites, numOfVariable):
        oddVars = [Variable("x"+str(i), 10*i) for i in range(1,numOfVariable+1) if not i%2]

        for site in range(1, numOfSites+1):
            evenVars = [Variable("x"+str(i), 10*i, site) for i in range(1,numOfVariable+1) if not i%2]
            oddVars = [Variable("x"+str(i), 10*i) for i in range(1,numOfVariable+1) if i%2 and (i%10 +1) == site]
            curSite = Site(str(site), evenVars+oddVars)
            curSite.isActive = True
            self.sites[str(site)] = curSite

    def recover(self, siteNum, tick):
        """
        Recover site

        Parameters:
            siteNum: Site number
            tick: cur tick
        """
        self.sites[siteNum].recover(tick)
        logger.debug(f"{tick}: Recovered Site {siteNum}")

    def fail(self, siteNum, tick):
        """
        Fail site

        Parameters:
            siteNum: Site number
        """
        self.sites[siteNum].fail(tick)
        logger.debug(f"{tick}: Failed Site {siteNum}")

    def get_site_index(self, x): 
        """
        Check the index of the variable. If even, return None; 
        if odd, return 1+index%10

        Parameters:
            x: variable name(with information of variable index)
        Returns: 
            None if x is on multiple sites; index if x is not replicated 
        """
        siteNum = int(re.split('(\d+)',x)[1])
        # siteNum = int(x[x.find('.')+1:])
        if siteNum % 2:
            return str(siteNum+1)

        return None


    def get_available_sites(self): 
        """
        Return a list of sites that are up.

        Returns: 
            A list of available sites.
        """

        return [site for site in self.sites.values() if site.isActive]
    
    def get_available_sites_for_variable(self, x): 
        """
        Get all sites that is active and contain x.

        Parameters:
            x: variable name
        Returns:
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
    
    def request_read_only(self, transaction, x):
        """
        Request read only operation.

        Parameters:
            transaction: transaction object
            x: variable name
        Returns:
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
            return sites[0].read(transaction, x)
        
        # Replicated variable
        for site in sites:
            if site.if_available_to_read(transaction, x):
                # read the first available  
                return site.read(transaction, x)

        logger.debug(f"{transaction.name} fail to read only on variable {x}! Can't be read on sites {sites}.")
        return None

    def request_read(self, transaction, x, tick):
        """
        Request read operation.

        Parameters:
            transaction: transaction object
            x: variable name
            tick: current tick
        Returns:
            The value of variable x if read successfully; None otherwise
        """

        logger.debug(f"{transaction.name} requests read on variable {x}.")
        sites = self.get_available_sites_for_variable(x)
        if not sites:
            # All sites 
            logger.debug(f"{transaction.name} fail to read on variable {x}! No active sites.")
            return (False, [])

        blocked = []
        for site in sites:
            if site.if_available_to_read(transaction, x):
                blocked = site.lock_variable(transaction, x, LockState.R_LOCK, tick)
                if not blocked:
                    return (True, site.read(transaction, x))
                
        logger.debug(f"{transaction.name} fail to read on variable {x}! Can't be read on sites {sites}.")
        return (False, blocked)

    def request_write(self, transaction, x, val, tick):
        """
        Request write operation.

        Parameters:
            transaction: transaction object
            x: name of the variable to write
            val: the value to write
            tick: current tick
        Returns:
            True if success; false if fail
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

        Parameters:
            transaction: transaction object
        """

        logger.debug(f"Abort {transaction.name} on all sites.")

        sites = self.get_available_sites()
        for site in sites:
            site.abort(transaction)