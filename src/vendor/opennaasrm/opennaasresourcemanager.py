import amsoil.core.pluginmanager as pm
import amsoil.core.log

from opennaasexceptions import *
from utils.xrn import urn_to_hrn, hrn_to_urn, get_leaf

logger = amsoil.core.log.getLogger('opennaasresourcemanager')

worker = pm.getService('worker')
config = pm.getService('config')


''' @author: Hector Fernandez'''
''' @email: hbfernandezr@gmail.com'''


class OPENNAASResourceManager(object):

    EXPIRY_CHECK_INTERVAL = 10  # sec
    MAX_PROVISIONED_TIMEOUT = config.get('opennaas.provisionMaxTimeout')
    MAX_ALLOCATED_TIMEOUT = config.get('opennaas.allocationMaxTimeout')
    SHUTDOWN = 'geni_shutdown'

    def __init__(self):
        super(OPENNAASResourceManager, self).__init__()
        # register callback for regular updates
        #TODO hace falta una consola adicional para activar el worker
        #worker.addAsReccurring("opennaasresourcemanager", "expire_elements", None, self.EXPIRY_CHECK_INTERVAL)

    # Lease commands
    def get_all_leases(self):
        slice_names = db_session.query(OPENNAASLease.slice_name).group_by(OPENNAASLease.slice_name).all()

        # detach the objects from the database session, so the user can not directly change the database
        db_session.expunge_all()

        results = []
        for slice_name in slice_names:
            resources = db_session.query(OPENNAASLease).filter(OPENNAASLease.slice_name == slice_name[0]).all()
            db_session.expunge_all()
            results.append(resources)

        return results

    def reserve_lease(self, resType, name, slice_urn, owner_uuid, end_time=None):
        slice_hrn, hrn_type = urn_to_hrn(slice_urn)

        if hrn_type != 'slice' or slice_hrn is None:
            raise OpennaasMalformedUrn(slice_urn)

        slice_name = get_leaf(slice_hrn)

        lease = db_session.query(OPENNAASLease).filter(OPENNAASLease.name == name, OPENNAASLease.resType == resType, OPENNAASLease.slice_name == slice_name).first()

        if lease is not None:
            raise OpennaasLeaseAlreadyTaken(name)

        # change database entry
        logger.error('RESERVE NAME %s TYPE %s SLICE %s' % (name, resType, slice_name,))
        lease = OPENNAASLease(name=name, resType=resType, slice_name=slice_name, owner_uuid=owner_uuid, status='geni_allocated')
        lease.set_end_time_with_max(end_time, self.MAX_ALLOCATED_TIMEOUT)

        db_session.add(lease)
        db_session.commit()
        db_session.expunge_all()

        return lease

    def extend_lease(self, lease, end_time=None):
        lease = find_lease(lease.slice_name, lease.resType, lease.name)

        if lease.status == 'geni_allocated':
            result = lease.set_end_time_with_max(end_time, self.MAX_ALLOCATED_TIMEOUT)

        else:
            result = lease.set_end_time_with_max(end_time, self.MAX_PROVISIONED_TIMEOUT)

        db_session.commit()
        db_session.expunge_all()

        return result

    def changeStatus(self, lease, status):
        lease = find_lease(lease.slice_name, lease.resType, lease.name)
        lease.setStatus(status)

        db_session.commit()
        db_session.expunge_all()

    def leases_in_slice(self, slice_urn):
        slice_hrn, hrn_type = urn_to_hrn(slice_urn)

        if hrn_type != 'slice' or slice_hrn is None:
            raise OpennaasMalformedUrn(slice_urn)

        slice_name = get_leaf(slice_hrn)
        logger.info("SLICENAME %s" % (slice_name,))

        leases = db_session.query(OPENNAASLease).filter(OPENNAASLease.slice_name == slice_name).all()

        db_session.expunge_all()
        logger.info("LEASES IN SLICE (Res Manager) %s" % (leases,))
        return leases

    def find_lease_from_urn(self, sliver_urn):
        logger.info("FIND LEASE FROM URN SLIVER URN %s" % (sliver_urn,))
        sliver_hrn, hrn_type = urn_to_hrn(sliver_urn)

        logger.info("SLIVER HRN %s " % (sliver_hrn,))

        lease_slice_name, lease_type, lease_name = sliver_hrn.split('.')
        logger.info("SLICE NAME %s ---- TYPE %s ---- NAME %s" % (lease_slice_name, lease_type, lease_name,))

        return find_lease(lease_slice_name, lease_type, lease_name)

    def free_lease(self, lease):
        logger.info("FREE LEASE %s" % (lease.name,))
        lease = find_lease(lease.slice_name, lease.resType, lease.name)

        db_session.delete(lease)
        db_session.commit()

        return None

    def undo_allocate(self, lease):
        self.free_lease(lease)

    def undo_free(self, lease):
        new_lease = OPENNAASLease(name=lease.name, resType=lease.resType, slice_name=lease.slice_name, owner_uuid=lease.owner_uuid, status=lease.status)
        new_lease.setEndTime(lease.end_time)

        db_session.add(new_lease)
        db_session.commit()
        db_session.expunge_all()

    def shutdown(self, lease):
        lease = find_lease(lease.slice_name, lease.resType, lease.name)
        lease.setStatus(self.SHUTDOWN)

        db_session.commit()
        db_session.expunge_all()

        return True

    @worker.outsideprocess
    def expire_elements(self, params):
        leases = db_session.query(OPENNAASLease).filter(OPENNAASLease.end_time < datetime.utcnow()).all()

        for lease in leases:
            logger.info("Removing expired OPENNAAS elements: %s(%s) on slice %s" % (lease.name, lease.resType, lease.slice_name,))
            self.free_lease(lease)
        return


# ----------------------------------------------------
# ------------------ database stuff ------------------
# ----------------------------------------------------
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from amsoil.config import expand_amsoil_path
from datetime import datetime, timedelta

# initialize sqlalchemy
OPENNAASDB_PATH = expand_amsoil_path(pm.getService('config').get('opennaas.dbpath'))
OPENNAASDB_ENGINE = "sqlite:///%s" % (OPENNAASDB_PATH,)

db_engine = create_engine(OPENNAASDB_ENGINE, pool_recycle=6000)  # please see the wiki for more info
db_session_factory = sessionmaker(autoflush=True, bind=db_engine, expire_on_commit=False)  # the class which can create sessions (factory pattern)
db_session = scoped_session(db_session_factory)  # still a session creator, but it will create _one_ session per thread and delegate all method calls to it
Base = declarative_base()  # get the base class for the ORM, which includes the metadata object (collection of table descriptions)

# We should limit the session's scope (lifetime) to one request. Yet, here we have a different solution.
# In order to avoid side effects (from someone changing a database object), we expunge_all() objects when we hand out objects to other classes.
# So, we can leave the session as it is, because there are no objects in it anyway.


class OPENNAASLease(Base):
    """Please see the Database wiki page."""
    __tablename__ = 'leases'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String)
    resType = Column(String)
    slice_name = Column(String)
    owner_uuid = Column(String)
    status = Column(String)
    end_time = Column(DateTime)

    @property
    def _hrn(self):
        logger.error('_HRN SLICE NAME %s TYPE %s NAME %s' % (self.slice_name, self.resType, self.name,))
        return self.slice_name + '.' + self.resType + '.' + self.name

    @property
    def _urn(self):
        return hrn_to_urn(self._hrn, 'sliver')

    def setStatus(self, status):
        logger.error('STATUS CHANGED: %s' % (status,))
        self.status = status

    def setEndTime(self, endtime):
        logger.error('ENDTIME CHANGED: %s' % (endtime,))
        self.end_time = endtime

    def set_end_time_with_max(self, end_time, max_duration):
        """If {end_time} is none, the current time+{max_duration} is assumed."""
        logger.info("END TIME %s" % (end_time,))
        logger.info("max_duration %s" % (max_duration,))
        max_end_time = datetime.utcnow() + timedelta(0, max_duration)
        logger.info("max_end_time %s" % (max_end_time,))
        logger.info("DATE UTC NOW %s" % (datetime.utcnow(),))
        logger.info("TIMEDELTA %s" % (timedelta(0, max_duration),))
        if end_time is None or end_time < datetime.utcnow():
            end_time = max_end_time
        if (end_time > max_end_time):
            raise OpennaasMaxLeaseDurationExceeded(self.name)
        self.end_time = end_time

        return end_time

Base.metadata.create_all(db_engine)  # create the tables if they are not there yet


def find_lease(slice_name, resType, resName):
    try:
        result = db_session.query(OPENNAASLease).filter(OPENNAASLease.slice_name == slice_name, OPENNAASLease.name == resName, OPENNAASLease.resType == resType).one()
    except NoResultFound:
        raise OpennaasLeaseNotFound(resName, resType)

    return result