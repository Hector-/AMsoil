import amsoil.core.pluginmanager as pm
from opennaasdelegate import OPENNAASGENI3Delegate


def setup():
    delegate = OPENNAASGENI3Delegate()
    handler = pm.getService('geniv3handler')
    handler.setDelegate(delegate)
