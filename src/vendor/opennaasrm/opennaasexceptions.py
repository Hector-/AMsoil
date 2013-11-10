from amsoil.core.exception import CoreException


''' @author: Hector Fernandez'''
''' @email: hbfernandezr@gmail.com'''


class OPENNAASException(CoreException):
    def __init__(self, desc):
        self._desc = desc

    def __str__(self):
        return "OpenNaas: %s" % (self._desc,)


class OpennaasMalformedUrn(OPENNAASException):
    def __init__(self, urn):
        super(OpennaasMalformedUrn, self).__init__("The urn hasn't the expected format (%s)." % (urn))


class OpennaasLeaseNotFound(OPENNAASException):
    def __init__(self, name, resType):
        super(OpennaasLeaseNotFound, self).__init__("Lease %s(%s) not found." % (name, resType))


class OpennaasLeaseAlreadyTaken(OPENNAASException):
    def __init__(self, name):
        super(OpennaasLeaseAlreadyTaken, self).__init__("Lease is already taken (%s)" % (name,))


class OpennaasMaxLeaseDurationExceeded(OPENNAASException):
    def __init__(self, name):
        super(OpennaasMaxLeaseDurationExceeded, self).__init__("Desired lease duration is too far in the future (%s)" % (name,))