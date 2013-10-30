import amsoil.core.pluginmanager as pm
import amsoil.core.log

from lxml import etree
from lxml.builder import ElementMaker
from utils.xrn import urn_to_hrn, get_leaf

logger = amsoil.core.log.getLogger('opennaasgeni3delegate')
GENIv3DelegateBase = pm.getService('geniv3delegatebase')
geni_ex = pm.getService('geniv3exceptions')
opennaas_ex = pm.getService('opennaasexceptions')
commands = pm.getService('opennaas_commands')


''' @author: Héctor Fernández'''
''' @email: hbfernandezr@gmail.com'''


class OPENNAASGENI3Delegate(GENIv3DelegateBase):
    """
    """

    URN_PREFIX = 'OPENNAAS_AM_'
    DESCRIBE_CALL = False

    def __init__(self):
        super(OPENNAASGENI3Delegate, self).__init__()
        self._resource_manager = pm.getService("opennaasresourcemanager")

    def get_request_extensions_mapping(self):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        return {'opennaas': 'http://extensionsExample.com/opennaas'}  # /request.xsd

    def get_manifest_extensions_mapping(self):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        return {'opennaas': 'https://github.com/Hector-/ocf-schemas/blob/master/manifest_schema.xsd'}  # /manifest.xsd

    def get_ad_extensions_mapping(self):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        return {'opennaas': 'https://github.com/Hector-/ocf-schemas/blob/master/adv_schema.xsd'}  # /ad.xsd

    def is_single_allocation(self):
        return False

    def get_allocation_mode(self):
        return 'geni_many'

    #TODO: AUTH + SCHEMA CHECK!!
    def list_resources(self, client_cert, credentials, geni_available):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""

        res_list = commands.commandsMngr.getResources()
        root_node = self.lxml_ad_root()
        E = self._lxml_ad_element_maker()
        resByType_ = E.resourcesByType()

        for resTypes_ in res_list:
            type_ = E.type()
            type_.set('name', resTypes_[0])

            for resources_ in resTypes_[1]:
                res_ = E.resource()
                res_.append(E.name(resources_))
                available = commands.commandsMngr.checkAvailability(resTypes_[0], resources_)
                res_.append(E.available("True" if available is True else "False"))

                if available is True:
                    connections_ = E.activeConnections()
                    connects = commands.commandsMngr.getXConnections(resTypes_[0], resources_)

                    for con_ in connects:
                        connections_.append(E.connection(con_))

                    res_.append(connections_)

                    endpoints_AD = E.availableEndPoints()
                    endpoints_O = E.availableEndPoints()
                    labels_AD = E.availableLabels()
                    labels_O = E.availableLabels()
                    eps = commands.commandsMngr.getEndPoints(resTypes_[0], resources_)

                    AD_firstime = True
                    O_firstime = True

                    for ep_ in eps:
                        lbs = commands.commandsMngr.getLabels(resTypes_[0], resources_, ep_)

                        if len(lbs) > 45:
                            endpoints_O.append(E.endPoint(ep_))

                            if O_firstime is True:
                                O_firstime = False
                                labels_O.append(E.fromLabel(lbs[0]))
                                labels_O.append(E.toLabel(lbs[-1]))

                        else:
                            endpoints_AD.append(E.endPoint(ep_))

                            if AD_firstime is True:
                                AD_firstime = False
                                for lb_ in lbs:
                                    labels_AD.append(E.label(lb_))

                    endpoints_AD.append(labels_AD)
                    endpoints_O.append(labels_O)

                    res_.append(endpoints_AD)
                    res_.append(endpoints_O)

                type_.append(res_)

                resByType_.append(type_)

            root_node.append(resByType_)

        return self.lxml_to_string(root_node)

    def describe(self, urns, client_cert, credentials):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN DESCRIBE %s' % (urns,))
        self.DESCRIBE_CALL = True
        rspec, sliver_list = self.status(urns, client_cert, credentials)
        self.DESCRIBE_CALL = False
        return rspec

    #TODO AUTH + SCHEMA CHECK
    def allocate(self, slice_urn, client_cert, credentials, rspec, end_time=None):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN ALLOCATE %s' % (slice_urn,))
        logger.error('PINTA DEL END-TIME EN ALLOCATE %s' % (end_time,))

        # AUTH
        #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, slice_urn, ('createsliver',))

        reserved_leases = []

        # parse
        root = etree.fromstring(rspec)
        if root.tag != 'rspec':
            raise geni_ex.GENIv3BadArgsError("RSpec seems not to be well-formed (%s)" % (root.tag,))

        for resourceTypes in root:
            if resourceTypes.tag != 'resourcesByType':
                raise geni_ex.GENIv3BadArgsError("RSpec seems not to be well-formed (%s)" % (resourceTypes.tag,))
            else:
                for resType in resourceTypes:
                    if resType.tag != 'type':
                        raise geni_ex.GENIv3BadArgsError("RSpec seems not to be well-formed (%s)" % (resType.tag,))

                    for resources in resType:
                        if resources.tag != 'resource':
                            raise geni_ex.GENIv3BadArgsError("RSpec seems not to be well-formed (%s)" % (resources.tag,))

                        res_name = resources.find('name').text
                        res_available = resources.find('available').text
                        try:
                            # reserve resources
                            if res_available == 'True':
                                lease = self._resource_manager.reserve_lease(resType.attrib["name"], res_name, slice_urn, 'TODO', end_time)

                                connections = resources.find('activeConnections')
                                original_connections = commands.commandsMngr.getXConnections(resType.attrib["name"], res_name)

                                if 'Error' in original_connections:
                                    self._rollback_allocates(slice_urn)
                                    raise geni_ex.GENIv3BadArgsError("You are trying to allocate a resource that seems is not available (%s %s)" % (resType.attrib["name"], res_name,))

                                try:
                                    for con in connections:
                                        if con.text in original_connections:
                                            original_connections.remove(con.text)
                                        else:
                                            # make XConnection
                                            logger.info("Creating XConnection %s from ROADM %s" % (con.text, res_name,))
                                            params = commands.commandsMngr.splitXConnectionId(con.text)
                                            commands.commandsMngr.makeXConnection(resType.attrib["name"], res_name, con.text,
                                                                                    params[0], params[1], params[2], params[3])

                                    for con_ in original_connections:
                                        # remove XConnection
                                        logger.info("Removing XConnection from ROADM %s" % (con_,))
                                        commands.commandsMngr.removeXConnection(resType.attrib["name"], res_name, con_)

                                    reserved_leases.append({'slice_urn': slice_urn, 'slice_name': lease.slice_name, 'name': lease._urn, 'resType': lease.resType,
                                                                'expires': lease.end_time, 'status': lease.status})
                                except:
                                    #TODO Tras el lock del recurso aqui tendria que haber un rollback de las conexiones hechas
                                    self._rollback_allocates(slice_urn)
                                    raise geni_ex.GENIv3BadArgsError("You are trying to create/remove a connection that is not well-formed.")
                            else:
                                self._rollback_allocates(slice_urn)
                                raise geni_ex.GENIv3UnavailableError("The resource you are trying to allocate is not available, please select another (%s %s)" % (resType.attrib["name"], res_name,))
                        except opennaas_ex.OpennaasMalformedUrn:
                            raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs are admited in this AM')
                        except opennaas_ex.OpennaasLeaseAlreadyTaken:
                            raise geni_ex.GENIv3AlreadyExistsError("The desired resource is already taken (%s %s)" % (resType.attrib["name"], res_name,))
                        except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                            raise geni_ex.GENIv3BadArgsError("Allocated lease(s) can not be extended that long (%s %s)" % (resType.attrib["name"], res_name,))

        sliver_list = [self._get_lease_status_hash(lease, True, False, "") for lease in reserved_leases]
        return self.lxml_to_string(self._get_manifest_rspec(reserved_leases)), sliver_list

    #TODO AUTH
    def renew(self, urns, client_cert, credentials, expiration_time, best_effort):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN RENEW %s' % (urns,))
        logger.error('BEST EFFORT RENEW %s' % (best_effort,))

        renewed_leases = []
        rollback_leases = []

        for urn in urns:
            if self.urn_type(urn) == 'slice':
                #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, urn, ('renewsliver',))
                slice_leases = self._resource_manager.leases_in_slice(urn)

                if len(slice_leases) == 0 and best_effort is True:
                    renewed_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None, 'error': 'There are no resources in the given slice'})

                else:
                    for lease in slice_leases:
                        if lease.status == self.ALLOCATION_STATE_ALLOCATED:
                            try:
                                # renew allocated resource
                                rollback_leases.append([lease, lease.end_time])
                                end_time = self._resource_manager.extend_lease(lease, expiration_time)
                                renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': end_time, 'status': lease.status})

                            except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                                if best_effort is True:
                                    renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                            'error': 'Allocated lease(s) can not be extended that long'})
                                else:
                                    if len(rollback_leases) != 0:
                                        self._rollback_timeouts(rollback_leases,)
                                    raise geni_ex.GENIv3BadArgsError('Allocated lease(s) can not be extended that long (%s)' % (urn))

                        elif lease.status == self.ALLOCATION_STATE_PROVISIONED:
                            try:
                                # renew provisioned resource
                                rollback_leases.append([lease, lease.end_time])
                                end_time = self._resource_manager.extend_lease(lease, expiration_time)
                                renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': end_time, 'status': lease.status})

                            except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                                if best_effort is True:
                                    renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                            'error': 'Provisioned lease(s) can not be extended that long'})
                                else:
                                    if len(rollback_leases) != 0:
                                        self._rollback_timeouts(rollback_leases,)
                                    raise geni_ex.GENIv3BadArgsError('Provisioned lease(s) can not be extended that long (%s)' % (urn))

                        else:
                            # only allocated and provisioned resources can be renewed
                            if best_effort is True:
                                renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                            'error': 'Only allocated or provisioned leases can be renewed in this aggregate'})
                            else:
                                if len(rollback_leases) != 0:
                                    self._rollback_timeouts(rollback_leases,)
                                raise geni_ex.GENIv3OperationUnsupportedError('Only allocated or provisioned leases can be renewed in this aggregate (%s)' % (urn))

            elif self.urn_type(urn) == 'sliver':
                try:
                    lease = self._resource_manager.find_lease_from_urn(urn)
                    if lease.status == self.ALLOCATION_STATE_ALLOCATED:
                        try:
                            # renew allocated resource
                            rollback_leases.append([lease, lease.end_time])
                            end_time = self._resource_manager.extend_lease(lease, expiration_time)
                            renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': end_time, 'status': lease.status})

                        except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                            if best_effort is True:
                                renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                        'error': 'Allocated lease(s) can not be extended that long'})
                            else:
                                if len(rollback_leases) != 0:
                                    self._rollback_timeouts(rollback_leases,)
                                raise geni_ex.GENIv3BadArgsError('Allocated lease(s) can not be extended that long (%s)' % (urn))

                    elif lease.status == self.ALLOCATION_STATE_PROVISIONED:
                        try:
                            # renew provisioned resource
                            rollback_leases.append([lease, lease.end_time])
                            end_time = self._resource_manager.extend_lease(lease, expiration_time)
                            renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': end_time, 'status': lease.status})

                        except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                            if best_effort is True:
                                renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                        'error': 'Provisioned lease(s) can not be extended that long'})
                            else:
                                if len(rollback_leases) != 0:
                                    self._rollback_timeouts(rollback_leases,)
                                raise geni_ex.GENIv3BadArgsError('Provisioned lease(s) can not be extended that long (%s)' % (urn))

                    else:
                        # only allocated and provisioned resources can be renewed
                        if best_effort is True:
                            renewed_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status,
                                                        'error': 'Only allocated or provisioned leases can be renewed in this aggregate'})
                        else:
                            if len(rollback_leases) != 0:
                                self._rollback_timeouts(rollback_leases,)
                            raise geni_ex.GENIv3OperationUnsupportedError('Only allocated or provisioned leases can be renewed in this aggregate (%s)' % (urn))

                except opennaas_ex.OpennaasLeaseNotFound:
                    if best_effort is True:
                        renewed_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None, 'error': 'There desired resource urn could not be found'})
                    else:
                        if len(rollback_leases) != 0:
                            self._rollback_timeouts(rollback_leases)
                        raise geni_ex.GENIv3SearchFailedError("There desired resource urn could not be found (%s)" % (urn,))

            else:
                if best_effort is True:
                    renewed_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None,
                                            'error': 'Only slice or sliver URN(s) can be renewed in this aggregate'})
                else:
                    if len(rollback_leases) != 0:
                        self._rollback_timeouts(rollback_leases,)
                    raise geni_ex.GENIv3OperationUnsupportedError('Only slice or sliver URN(s) can be renewed in this aggregate')

        if len(renewed_leases) == 0:
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        slivers = list()
        for lease in renewed_leases:
            logger.error('lease %s' % (lease,))
            if 'error' in lease:
                sliver = self._get_lease_status_hash(lease, True, True, lease['error'])
            else:
                sliver = self._get_lease_status_hash(lease, True, True)
            slivers.append(sliver)

        logger.error('SLIVERS %s' % (slivers,))
        return slivers

    #TODO AUTH
    def provision(self, urns, client_cert, credentials, best_effort, end_time, geni_users):
        """Documentation see [geniv3rpc] GENIv3DelegateBase.
        {geni_users} is not relevant here."""
        logger.error('PINTA DE LA URN EN PROVISION %s' % (urns,))
        logger.error('BEST EFFORT PROVISION %s' % (best_effort,))

        provisioned_leases = []
        rollback_leases = []
        for urn in urns:
            if self.urn_type(urn) == 'slice':
                #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, urn, ('createsliver',))
                slice_leases = self._resource_manager.leases_in_slice(urn)

                if len(slice_leases) == 0 and best_effort is True:
                    provisioned_leases.append({'slice_urn': urn, 'name': urn, 'resType': None, 'expires': None, 'status': None,
                                                'error': 'There are no resources in the given slice'})

                else:
                    for lease in slice_leases:
                        if lease.status == self.ALLOCATION_STATE_ALLOCATED:
                            try:
                                # provision resource
                                rollback_leases.append([lease, lease.end_time])
                                expiration_time = self._resource_manager.extend_lease(lease, end_time)
                                self._resource_manager.changeStatus(lease, self.ALLOCATION_STATE_PROVISIONED)
                                provisioned_leases.append({'slice_urn': urn, 'name': lease._urn, 'resType': lease.resType, 'expires': expiration_time,
                                                            'status': self.ALLOCATION_STATE_PROVISIONED})

                            except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                                if best_effort is True:
                                    provisioned_leases.append({'slice_urn': urn, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time,
                                                                'status': lease.status, 'error': 'Provisioned lease(s) can not be extended that long'})
                                else:
                                    if len(rollback_leases) != 0:
                                        self._rollback_timeouts(rollback_leases, True)
                                    raise geni_ex.GENIv3BadArgsError('Provisioned lease(s) can not be extended that long (%s)' % (urn))

                        else:
                            # only allocated resources can be provisioned
                            if best_effort is True:
                                provisioned_leases.append({'slice_urn': urn, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time,
                                                            'status': lease.status, 'error': 'Only allocated leases can be provisioned on that AM'})
                            else:
                                if len(rollback_leases) != 0:
                                    self._rollback_timeouts(rollback_leases, True)
                                raise geni_ex.GENIv3BadArgsError('Only allocated leases can be provisioned on that AM (%s)' % (lease._urn))

            elif self.urn_type(urn) == 'sliver':
                try:
                    lease = self._resource_manager.find_lease_from_urn(urn)

                    if lease.status == self.ALLOCATION_STATE_ALLOCATED:
                        try:
                            # provision resource
                            rollback_leases.append([lease, lease.end_time])
                            expiration_time = self._resource_manager.extend_lease(lease, end_time)
                            self._resource_manager.changeStatus(lease, self.ALLOCATION_STATE_PROVISIONED)
                            provisioned_leases.append({'slice_urn': lease.slice_name, 'name': lease._urn, 'resType': lease.resType, 'expires': expiration_time,
                                                        'status': self.ALLOCATION_STATE_PROVISIONED})

                        except opennaas_ex.OpennaasMaxLeaseDurationExceeded:
                            if best_effort is True:
                                provisioned_leases.append({'slice_urn': lease.slice_name, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time,
                                                            'status': lease.status, 'error': 'Provisioned lease(s) can not be extended that long'})
                            else:
                                if len(rollback_leases) != 0:
                                    self._rollback_timeouts(rollback_leases, True)
                                raise geni_ex.GENIv3BadArgsError('Provisioned lease(s) can not be extended that long (%s)' % (urn))
                    else:
                        # only allocated resources can be provisioned
                        if best_effort is True:
                                    provisioned_leases.append({'slice_urn': lease.slice_name, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time,
                                                                'status': lease.status, 'error': 'Only allocated leases can be provisioned on that AM'})
                        else:
                            if len(rollback_leases) != 0:
                                self._rollback_timeouts(rollback_leases, True)
                            raise geni_ex.GENIv3BadArgsError('Only allocated leases can be provisioned on that AM (%s)' % (lease._urn))

                except opennaas_ex.OpennaasLeaseNotFound:
                    if best_effort is True:
                        provisioned_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None,
                                                    'error': 'There desired resource urn could not be found'})
                    else:
                        if len(rollback_leases) != 0:
                            self._rollback_timeouts(rollback_leases, True)
                        raise geni_ex.GENIv3SearchFailedError("There desired resource urn could not be found (%s)" % (urn,))

            else:
                if best_effort is True:
                    provisioned_leases.append({'slice_urn': urn, 'name': None, 'resType': None, 'expires': None, 'status': None,
                                                'error': 'Only slice URN(s) can be provisioned in this aggregate'})
                else:
                    if len(rollback_leases) != 0:
                        self._rollback_timeouts(rollback_leases, True)
                    raise geni_ex.GENIv3OperationUnsupportedError('Only slice or sliver URN(s) can be provisioned in this aggregate')

        if len(provisioned_leases) == 0:
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s); perform allocate first")

        # assemble return values
        slivers = list()
        for lease in provisioned_leases:
            logger.error('lease %s' % (lease,))
            if 'error' in lease:
                sliver = self._get_lease_status_hash(lease, True, True, lease['error'])
            else:
                sliver = self._get_lease_status_hash(lease, True, True)
            slivers.append(sliver)

        logger.error('SLIVERS %s' % (slivers,))
        return self.lxml_to_string(self._get_manifest_rspec(provisioned_leases)), slivers

    #TODO AUTH
    def status(self, urns, client_cert, credentials):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN STATUS %s' % (urns,))

        status_leases = []
        for urn in urns:
            if self.urn_type(urn) == 'slice':
                #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, urn, ('sliverstatus',)) # authenticate for each given slice
                slice_leases = self._resource_manager.leases_in_slice(urn)

                for lease in slice_leases:
                    status_leases.append({'slice_urn': urn, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status})
                    logger.error('URN: %s --- NAME: %s --- TYPE: %s ---- EXPIRES: %s ---- STATUS:%s' % (urn, lease._urn, lease.resType, lease.end_time, lease.status,))

            elif self.urn_type(urn) == 'sliver':
                try:
                    lease = self._resource_manager.find_lease_from_urn(urn)
                    status_leases.append({'slice_urn': urn, 'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': lease.status})
                    logger.error('URN: %s --- NAME: %s --- TYPE: %s ---- EXPIRES: %s ---- STATUS:%s' % (urn, lease._urn, lease.resType, lease.end_time, lease.status,))

                except opennaas_ex.OpennaasLeaseNotFound:
                    raise geni_ex.GENIv3SearchFailedError("There desired resource urn could not be found (%s)" % (urn,))

            else:
                raise geni_ex.GENIv3OperationUnsupportedError('Only slice or sliver URN(s) are allowed in this aggregate (%s)' % (urn))

        if len(status_leases) == 0:
            raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice(s)")

        sliver_list = [self._get_lease_status_hash(lease, True, True) for lease in status_leases]
        return self.lxml_to_string(self._get_manifest_rspec(status_leases)), sliver_list

    def perform_operational_action(self, urns, client_cert, credentials, action, best_effort):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN PERFORM %s' % (urns,))
        logger.error('PINTA DE LA ACTION EN PERFORM %s' % (action,))
        logger.error('BEST EFFORT PERFORM %s' % (best_effort,))

        raise geni_ex.GENIv3OperationUnsupportedError("POA command is not supported right now in this aggregate")

    #TODO AUTH
    def delete(self, urns, client_cert, credentials, best_effort):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN DELETE %s' % (urns,))
        logger.error('BEST EFFORT DELETE %s' % (best_effort,))

        deleted_leases = []
        rollback_leases = []
        for urn in urns:
            if self.urn_type(urn) == 'slice':
                #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, urn, ('deletesliver',)) # authenticate for each given slice
                slice_leases = self._resource_manager.leases_in_slice(urn)

                if len(slice_leases) == 0 and best_effort is True:
                    deleted_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None,
                                            'error': 'There are no resources in the given slice'})

                else:
                    for lease in slice_leases:
                        # delete resource
                        self._resource_manager.free_lease(lease)
                        deleted_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': self.ALLOCATION_STATE_UNALLOCATED})
                        rollback_leases.append(lease)

            elif self.urn_type(urn) == 'sliver':
                try:
                    # delete resource
                    lease = self._resource_manager.find_lease_from_urn(urn)
                    self._resource_manager.free_lease(lease)
                    deleted_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': self.ALLOCATION_STATE_UNALLOCATED})
                    rollback_leases.append(lease)

                except opennaas_ex.OpennaasLeaseNotFound:
                    if best_effort is True:
                        deleted_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None,
                                                'error': 'There desired resource urn could not be found'})
                    else:
                        if len(rollback_leases) != 0:
                            self._rollback_deletes(rollback_leases)
                        raise geni_ex.GENIv3SearchFailedError('There desired resource urn could not be found (%s)' % (urn,))

            else:
                if best_effort is True:
                    deleted_leases.append({'name': urn, 'resType': None, 'expires': None, 'status': None,
                                            'error': 'Only slice URN(s) can be deleted in this aggregate'})
                else:
                    if len(rollback_leases) != 0:
                        self._rollback_deletes(rollback_leases)
                    raise geni_ex.GENIv3OperationUnsupportedError('Only slice or sliver URN(s) can be deleted in this aggregate (%s)' % (urn,))

        if len(deleted_leases) == 0:
            raise geni_ex.GENIv3SearchFailedError('There are no resources in the given slice')

        # assemble return values
        slivers = list()
        for lease in deleted_leases:
            logger.error('lease %s' % (lease,))
            if 'error' in lease:
                sliver = self._get_lease_status_hash(lease, True, False, lease['error'])
            else:
                sliver = self._get_lease_status_hash(lease, True, False)
            slivers.append(sliver)

        logger.error('SLIVERS %s' % (slivers,))
        return slivers

    #TODO AUTH
    def shutdown(self, slice_urn, client_cert, credentials):
        """Documentation see [geniv3rpc] GENIv3DelegateBase."""
        logger.error('PINTA DE LA URN EN SHUTDOWN %s' % (slice_urn,))

        shutdown_leases = []
        if self.urn_type(slice_urn) == 'slice':
            #client_urn, client_uuid, client_email = self.auth(client_cert, credentials, urn, ('sliverstatus',)) # authenticate for each given slice
            slice_leases = self._resource_manager.leases_in_slice(slice_urn)

            if len(slice_leases) == 0:
                raise geni_ex.GENIv3SearchFailedError("There are no resources in the given slice")

            else:
                # shutdown resource
                for lease in slice_leases:
                    self._resource_manager.shutdown(lease)
                    shutdown_leases.append({'name': lease._urn, 'resType': lease.resType, 'expires': lease.end_time, 'status': 'geni_shutdown'})

        else:
            raise geni_ex.GENIv3OperationUnsupportedError('Only slice URNs can be given to shutdown in this aggregate (%s)' % (slice_urn,))

        sliver_list = [self._get_lease_status_hash(lease) for lease in shutdown_leases]
        return sliver_list

# Support methods

    # if an error occurs when allocating resources to a slice deletes already allocated ones
    def _rollback_allocates(self, slice_urn):
        logger.error('Errors found -- allocate rollback begins %s' % (slice_urn,))
        leases = self._resource_manager.leases_in_slice(slice_urn)
        for lease in leases:
            self._resource_manager.undo_allocate(lease)

    # if an error occurs when provisioning or renewing resources it rollbacks already changed end_time (and status if provision) from resources
    def _rollback_timeouts(self, leases, provisionedLeases=False):
        logger.error('Errors found -- timeouts rollback begins %s' % (leases,))
        for lease in leases:
            logger.error('ROLLBACK TIMEOUTS LEASE [0] %s' % (lease[0],))
            if provisionedLeases is True:
                self._resource_manager.extend_lease(lease[0], lease[1])
                self._resource_manager.changeStatus(lease[0], self.ALLOCATION_STATE_ALLOCATED)
            else:
                self._resource_manager.extend_lease(lease[0], lease[1])

    # if an error occurs when deleting resources from a slice it inserts again all deleted resources
    def _rollback_deletes(self, leases):
        logger.error('Errors found -- delete rollback begins %s' % (leases,))
        for lease in leases:
            self._resource_manager.undo_free(lease)

    def _get_lease_status_hash(self, lease, include_allocation_status=False, include_operational_status=False, error_message=None):
        """Helper method to create the sliver_status return values of allocate and other calls."""
        logger.error('ERROR MESSAGE GET STATUS HASH %s' % (error_message,))
        logger.error('LEASE GET STATUS HASH %s' % (lease,))

        result = {'geni_sliver_urn': lease['name'],
                  'geni_sliver_resource_type': lease['resType'],
                  'geni_expires': lease['expires']}

        if include_allocation_status:
                result['geni_allocation_status'] = lease['status']

        if include_operational_status:

            if lease['status'] == self.ALLOCATION_STATE_ALLOCATED or lease['status'] == 'geni_shutdown':
                result['geni_operational_status'] = self.OPERATIONAL_STATE_NOTREADY

            elif error_message:
                result['geni_operational_status'] = self.OPERATIONAL_STATE_FAILED

            else:
                result['geni_operational_status'] = self.OPERATIONAL_STATE_READY

        if (error_message):
            result['geni_error'] = error_message

        return result

    #TODO subir el schema del manifest
    def _get_manifest_rspec(self, leases):
        manifest = self.lxml_manifest_root()
        E = self._lxml_manifest_element_maker()

        temp_lease = leases[0]

        slice_hrn, hrn_type = urn_to_hrn(temp_lease['slice_urn'])
        slice_name = get_leaf(slice_hrn)

        slices = E.slices()
        _slice = E.slice()
        _slice.append(E.name(slice_name))

        resources = E.resources()

        for lease in leases:
            res = E.resource()
            res.append(E.name(lease['name']))
            res.append(E.type(lease['resType']))

            if self.DESCRIBE_CALL is True:
                resource = self._resource_manager.find_lease_from_urn(lease['name'])
                connections = commands.commandsMngr.getXConnections(resource.resType, resource.name)

                connections_ = E.availableConnections()

                if not 'Error' in connections:
                    for con_ in connections:
                        connections_.append(E.connection(con_))

                res.append(connections_)

            res.append(E.status(lease['status']))
            res.append(E.expires(str(lease['expires'])))

            resources.append(res)

        _slice.append(resources)
        slices.append(_slice)
        manifest.append(slices)

        return manifest

    def _lxml_ad_element_maker(self):
        """Returns a lxml.builder.ElementMaker configured for avertisements and the namespace given by {prefix}."""
        return ElementMaker()

    def _lxml_manifest_element_maker(self):
        """Returns a lxml.builder.ElementMaker configured for manifests and the namespace given by {prefix}."""
        return ElementMaker()