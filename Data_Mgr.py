import Site
import Variable

class DataMgr(object):
    def __init__(self, numOfSites, numOfVariable) -> None:
        self.sites = {}
        self.init_sites(numOfSites, numOfVariable)

    def init_sites(self, numOfSites, numOfVariable):
        oddVars = [Variable("x."+str(i), 10*i) for i in range(1,numOfVariable+1) if not i%2]

        for site in range(1, numOfSites+1):
            evenVars = [Variable("x."+str(i), 10*i, site) for i in range(1,numOfVariable+1) if not i%2]
            oddVars = [Variable("x."+str(i), 10*i) for i in range(1,numOfVariable+1) if i%2 and (i%10 +1) == site]
            curSite = Site(evenVars+oddVars)
            self.sites[site] = curSite

    def recover(self, siteNum, tick):
        """
        Recover site

        Parameters:
            siteNum: Site number
            tick: cur tick
        """

        self.sites[siteNum].recover(tick)

    def fail(self, siteNum):
        """
        Fail site

        Parameters:
            siteNum: Site number
        """
        self.sites[siteNum].fail()

    def get_site_index(x): 
        """
            By checking the index of the variable if even, return None; 
            if odd, return 1+index%10

            Parameters:
                x: variable name(with information of variable index)
            Returns: 
                None if x is on multiple sites; index if x is not replicated 
        """

        siteNum = int(x[x.find('.')+1:])
        if siteNum % 2:
            return siteNum+1

        return None


    def get_available_sites(self): 
        """
        Return a list of sites that are up.

        Returns: 
            A list of available sites.
        """

        return [site for site in self.sites.values() if site.isActive()]
    
    def get_available_sites_for_variable(self, x): 
        """
        Parameters:
            x: variable name
        Returns:
            The list of sites that are up and contain x.

        1)	Call GetSiteIndex() to get the index of sites containing x.
        2)	If return none, x is on all sites. Call GetAvailableSites() to get a list of available sites. And return the list.
        3)	Otherwise, return the index if the site is up.
        """

        varNum = int(x[x.find('.')+1:])
        siteIndex = self.get_site_index(x)
        if siteIndex:
            # odd variable only on one site
            site=self.sites[siteIndex]
            if site.isActive() and site.ifContains(x):
                return [site]
            return []
        
        # even variable, can have multiple copies
        avaliableSites = self.get_available_sites()
        allSites = [site for site in avaliableSites if site.ifContains(x)]

        return allSites
    





    
        
