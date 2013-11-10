import amsoil.core.pluginmanager as pm
import opennaasexceptions as exceptions_package
import amsoil.core.log
logger = amsoil.core.log.getLogger('opennaasresourcemanager')

config = pm.getService('config')

import requests
import xml.etree.ElementTree as ET

"""
OpenNaas Commands Manager.
author: Roberto Monno
"""


class CommandsManager(object):
    """ Resource commands """

    def __init__(self, host, port):
        self._base_url = 'http://' + host + ':' + port + '/opennaas/'
        self._auth = (config.get("opennaas.user"), config.get("opennaas.password"))

    def post(self, url, xml_data):
        try:
            logger.debug("POST url=%s, data=%s" % (url, xml_data,))
            resp_ = requests.post(url=url, headers={'Content-Type': 'application/xml'},
                                  auth=self._auth, data=xml_data).text
            logger.debug("POST resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise exceptions_package.OPENNAASException(str(e))

    def get(self, url):
        try:
            logger.debug("GET url=%s" % (url,))
            resp_ = requests.get(url=url, auth=self._auth).text
            logger.debug("GET resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise exceptions_package.OPENNAASException(str(e))

    def delete(self, url):
        try:
            logger.debug("DELETE url=%s" % (url,))
            resp_ = requests.delete(url=url, auth=self._auth).text
            logger.debug("DELETE resp=%s" % (resp_,))
            return resp_

        except requests.exceptions.RequestException as e:
            raise exceptions_package.OPENNAASException(str(e))

    def decode_xml_entry(self, xml_data):
        try:
            return [e.text.strip() for e in ET.fromstring(xml_data).findall('entry')]

        except ET.ParseError as e:
            logger.error("XML ParseError: %s" % (str(e),))
            return []

    def getResources(self):
        ret_ = []
        command = 'resources/getResourceTypes'
        ts_ = self.get(self._base_url + command)
        for t in self.decode_xml_entry(ts_):
            command = 'resources/listResourcesByType/' + t
            ns_ = self.get(self._base_url + command)
            res_ = []
            for n in self.decode_xml_entry(ns_):
                res_.append(n)
            if len(res_) != 0:
                ret_.extend([(t, res_)])

        return ret_


class RoadmCM(CommandsManager):
    """ Roadm specific commands """

    def __init__(self, host, port):
        super(RoadmCM, self).__init__(host, port)

    # UTILS
    def decode_xml_conn(self, xml_data):
        try:
            return (ET.fromstring(xml_data).find('instanceID').text,
                    ET.fromstring(xml_data).find('srcEndPointId').text,
                    ET.fromstring(xml_data).find('srcLabelId').text,
                    ET.fromstring(xml_data).find('dstEndPointId').text,
                    ET.fromstring(xml_data).find('dstLabelId').text)

        except ET.ParseError as e:
            logger.error("XML ParseError: %s" % (str(e),))
            return None

    def encode_xml_conn(self, x_id, src_ep, src_label, dst_ep, dst_label):
        root = ET.Element('xConnection')
        ET.SubElement(root, 'instanceID').text = x_id
        ET.SubElement(root, 'srcEndPointId').text = src_ep
        ET.SubElement(root, 'srcLabelId').text = src_label
        ET.SubElement(root, 'dstEndPointId').text = dst_ep
        ET.SubElement(root, 'dstLabelId').text = dst_label

        return ET.tostring(root)

    # RESOURCES commands
    def listResourceTypes(self):
        command = 'resources/getResourceTypes/'
        res = self.get(self._base_url + command)
        return self.decode_xml_entry(res)

    def listResourcesByType(self, r_type):
        command = 'resources/listResourcesByType/' + r_type
        res = self.get(self._base_url + command)
        return self.decode_xml_entry(res)

    def getResourceId(self, r_type, r_name):
        command = 'resources/getId/' + r_type + '/' + r_name
        res = self.get(self._base_url + command)
        return res

    # ROADM commands
    def makeXConnection(self, r_type, r_name, instance_id,
                        src_ep_id, src_label_id,
                        dst_ep_id, dst_label_id):
        data = self.encode_xml_conn(instance_id, src_ep_id, src_label_id,
                                    dst_ep_id, dst_label_id)
        command = r_type + '/' + r_name + '/xconnect/'
        r_ = self.post(self._base_url + command, data)

        self.execute(r_type, r_name)

        return self.decode_xml_entry(r_)

    def removeXConnection(self, r_type, r_name, instance_id):
        command = r_type + '/' + r_name + '/xconnect/' + instance_id
        self.delete(self._base_url + command)
        self.execute(r_type, r_name)

    def getXConnections(self, r_type, r_name):
        command = r_type + '/' + r_name + '/xconnect/'
        try:
            cs_ = self.get(self._base_url + command)

        except:
            return ('Error')

        if 'Error' in cs_:
            return ('Error')

        else:
            return self.decode_xml_entry(cs_)

    def getXConnection(self, r_type, r_name, xconn_id):
        command = r_type + '/' + r_name + '/xconnect/' + xconn_id
        eps_ = self.get(self._base_url + command)
        return self.decode_xml_conn(eps_)

    def getEndPoints(self, r_type, r_name):
        command = r_type + '/' + r_name + '/xconnect/getEndPoints'
        eps_ = self.get(self._base_url + command)
        return self.decode_xml_entry(eps_)

    def getLabels(self, r_type, r_name, ep_id):
        command = r_type + '/' + r_name + '/xconnect/getLabels/' + ep_id
        ls_ = self.get(self._base_url + command)
        return self.decode_xml_entry(ls_)

    def checkAvailability(self, r_type, r_name):
        r_id = self.getResourceId(r_type, r_name)
        command = 'resources/getStatus/' + r_id
        return self.get(self._base_url + command) == 'ACTIVE'

    def splitXConnectionId(self, XConnectionId):
        result = []

        srcDst = XConnectionId.split('::')
        srcEP = srcDst[0].split(':')[0]
        srcLB = srcDst[0].split(':')[1]

        dstEP = srcDst[1].split(':')[0]
        dstLB = srcDst[1].split(':')[1]

        result.append(srcEP)
        result.append(srcLB)
        result.append(dstEP)
        result.append(dstLB)

        return result

    # QUEUE commands
    def execute(self, r_type, r_name):
        command = r_type + '/' + r_name + '/queue/execute'
        self.post(self._base_url + command, None)

commandsMngr = RoadmCM(host=config.get('opennaas.server_address'),
                       port=str(config.get('opennaas.server_port')))
