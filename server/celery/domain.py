import logging
logger = logging.getLogger('celery.task')
from celery import Task

class DomainBasedTask(Task):

    abstract = True
    _DomainCache = {}

    def __init__(self):
        pass

    @classmethod
    def createDomain(cls, domainSpec ):
        domain = Domain( domainSpec )
        id = cls.generateDomainId( domainSpec )
        cls._DomainCache[ id ] = domain
        logger.info( "Create domain '%s', Domain Cache: %s " % ( id, str( cls._DomainCache ) ) )
        return id

    def __repr__(self):
        return "DomainBasedTask ( Cache: %s )" % str( self._DomainCache )

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
        self.pIndex = spec['pIndex']
        self.roi = spec.get( 'roi', None )
        self.time = spec.get( 'time', None )
        self.grid = spec.get( 'grid', None )
        self.variables = {}

    def __repr__(self):
        return "Domain[%s] { roi: %s, grid: %s, time: %s } ( Variables: %s )" % ( self.id, self.roi, self.grid, self.time, self.variables.keys() )

    def add_variable( self, varId, variable, **args ):
        if self.time <> None:
            data_start = self.time['start'].split('-')
            part_time_step = self.time.get('step',1)
            data_start_year, data_start_month = int( data_start[0] ), int( data_start[1] )
            iA = self.pIndex / 12
            end_year = start_year = data_start_year + iA
            start_month = (self.pIndex % 12) + data_start_month
            end_month = start_month + part_time_step
            if end_month > 12:
                end_year = end_year + ( end_month - 1 ) / 12
                end_month = ( ( end_month - 1 ) % 12 ) + 1
            logger.info( 'Domain[%d]: addVariable: %s -> %s' % (self.pIndex, '%d-%d'%(start_year,start_month), '%d-%d'%(end_year,end_month) ))
            part_variable = variable( time=( '%d-%d'%(start_year,start_month), '%d-%d'%(end_year,end_month), 'co') )
            self.variables[varId] = part_variable

    def remove_variable( self, varId ):
        try:
            del self.variables[varId]
        except KeyError:
            logger.error( "Attempt to delete non-existent variable '%s' in domain '%s'" % ( varId, self.id ) )
