import logging
logger = logging.getLogger('celery.task')
from celery import Task

class DomainBasedTask(Task):

    _DomainCache = {}

    def __init__(self):
        pass

    @classmethod
    def createDomain(cls, domainSpec ):
        domain = Domain( domainSpec )
        id = cls.generateDomainId( domainSpec )
        cls._DomainCache[ id ] = domainSpec
        logger.info( "Create domain '%s', Domain Cache: %s " % ( id, str( cls._DomainCache ) ) )
        return id

    @classmethod
    def generateDomainId(cls, domainSpec ):
        return domainSpec['id']

    @classmethod
    def getDomain(cls, domainId ):
        logger.info( "Get domain '%s', Domain Cache: %s " % ( domainId, str( cls._DomainCache ) ) )
        return cls._DomainCache.get( domainId, None )

    @classmethod
    def removeDomain( cls, domainId ):
        try:
            del cls._DomainCache[domainId]
        except KeyError:
            logger.error( "Attempt to delete non-existent domain: %s" % ( domainId ) )


class Domain(object):

    def __init__(self, spec ):
        self.id = spec['id']
        self.roi = spec.get( 'roi', None )
        self.time_bounds = spec.get( 'time_bounds', None )
        self.grid = spec.get( 'grid', None )
        self.variables = {}

    def add_variable( self, varId, variable ):
        self.variables[varId] = variable

    def remove_variable( self, varId ):
        try:
            del self.variables[varId]
        except KeyError:
            logger.error( "Attempt to delete non-existent variable '%s' in domain '%s'" % ( varId, self.id ) )
