from Site import Site
from Variable import Variable
from const import R_LOCK, RW_LOCK
import logging
import re

logger = logging.getLogger(__name__)

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
        Parameters:
            x: variable name
        Returns:
            The list of sites that are up and contain x.
        """

        # varNum = int(x[x.find('.')+1:])
        siteIndex = self.get_site_index(x)
        if siteIndex:
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
            logger.debug(f"{transaction.name} fail to read only on variable {x}! No available sites.")
            return None

        if self.get_site_index(x):
            # odd variable
            return sites[0].read(transaction, x)
        
        # Replicated variable
        for site in sites:
            if site.if_available_to_read(transaction, x, ifLockNeeded=False):
                # read the first available  
                return site.read(transaction, x)

        logger.debug(f"{transaction.name} fail to read only on variable {x}! Can't be read on sites {sites}.")
        return None

    def request_read(self, transaction, x):
        """
        Request read operation.

        Parameters:
            transaction: transaction object
            x: variable name
        Returns:
            The value of variable x if read successfully; None otherwise
        """

        logger.debug(f"{transaction.name} requests read on variable {x}.")
        sites = self.get_available_sites_for_variable(x)
        if not sites:
            logger.debug(f"{transaction.name} fail to read on variable {x}! No available sites.")
            return None

        for site in sites:
            if site.if_available_to_read(transaction, x, ifLockNeeded=True):
                site.lock_variable(transaction, x, R_LOCK)
                return site.read(transaction, x) 

        logger.debug(f"{transaction.name} fail to read on variable {x}! Can't be read on sites {sites}.")
        return None


    
        
