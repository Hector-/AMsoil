import amsoil.core.pluginmanager as pm


def setup():

    # setup config keys
    config = pm.getService('config')
    config.install('opennaas.server_address', 'localhost', 'OpenNaas server address.')
    config.install('opennaas.server_port', 8888, 'OpenNaas server port.')
    config.install('opennaas.user', 'admin', 'OpenNaas user.')
    config.install('opennaas.password', '123456', 'OpenNaas password.')
    config.install('opennaas.provisionMaxTimeout', 300 * 60, 'Provision timeout (5 hours).')
    config.install('opennaas.allocationMaxTimeout', 120 * 60, 'Allocation timeout (2 hours).')
    config.install('opennaas.dbpath', 'deploy/opennaas.db', 'Path to the opennaas database (if relative, root will be assumed).')

    from opennaasresourcemanager import OPENNAASResourceManager
    import opennaasexceptions as exceptions_package
    import commandsmanager as commands

    rm = OPENNAASResourceManager()
    pm.registerService('opennaasresourcemanager', rm)
    pm.registerService('opennaasexceptions', exceptions_package)
    pm.registerService('opennaas_commands', commands)